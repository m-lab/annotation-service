package manager

// The AnnotatorCache is an LRU cache that handles loading and eviction of annotators in
// response to requests for specific dates.
// Configuration parameters are:
//   minEvictionAge - the minimum period that an annotator should be idle before eviction.
//   maxPendingLoads - the maximum number of concurrent loads allowed.
//   maxEntries - the maximum number of entries allowed, to avoid OOM problems.
//   loader - the function that handles loading of a new annotator.

// TODO - consider preloading the NEXT annotator whenever there is room available.

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geoloader"
	"github.com/m-lab/annotation-service/metrics"
)

// CacheError is the error type for all errors related to the Annotator cache.
type CacheError interface {
	error
}

func newCacheError(msg string) CacheError {
	return CacheError(errors.New(msg))
}

var (
	// ErrNilDataset is returned when CurrentAnnotator is nil.
	ErrNilDataset = newCacheError("CurrentAnnotator is nil")

	// ErrPendingAnnotatorLoad is returned when a new annotator is requested, but not yet loaded.
	ErrPendingAnnotatorLoad = newCacheError("annotator is loading")

	// ErrAnnotatorLoadFailed is returned when a requested annotator has failed to load.
	ErrAnnotatorLoadFailed = newCacheError("unable to load annotator")

	// These are UNEXPECTED errors!!
	// ErrGoroutineNotOwner is returned when goroutine attempts to set annotator entry, but is not the owner.
	ErrGoroutineNotOwner = newCacheError("goroutine not owner")
	// ErrMapEntryAlreadySet is returned when goroutine attempts to set annotator, but entry is non-null.
	ErrMapEntryAlreadySet = newCacheError("map entry already set")
	// ErrNilEntry is returned when map has a nil entry, which should never happen.
	ErrNilEntry = newCacheError("Map entry is nil")

	allAnnotators *AnnotatorCache
)

func errorMetricWithLabel(err error) {
	if err != nil {
		_, _, line, _ := runtime.Caller(1)
		label := fmt.Sprintf("%3d %s", line+2, err)
		metrics.ErrorTotal.WithLabelValues(label).Inc()
	}
}

// NOT THREADSAFE.  Should be called once at initialization time.
func SetAnnotatorCacheForTest(ac *AnnotatorCache) {
	allAnnotators = ac
}

type cacheEntry struct {
	ann api.Annotator
	// UPDATED ATOMICALLY, or read while holding exclusive AnnotatorCache lock.
	lastUsed unsafe.Pointer
	err      CacheError
}

func (ce *cacheEntry) updateLastUsed() {
	newTime := time.Now()
	atomic.StorePointer(&ce.lastUsed, unsafe.Pointer(&newTime))
}

func (ce *cacheEntry) getLastUsed() time.Time {
	ptr := atomic.LoadPointer(&ce.lastUsed)
	return *(*time.Time)(ptr)
}

// AnnotatorCache manages all loading and fetching of Annotators.
// TODO - should this be a generic cache of interface{}?
//
// Synchronization:
//  All accesses must hold the mutex.  If an element is not found, the goroutine
//  may take ownership of the loading job by writing a new empty entry into the
//  map.  It should then start a new goroutine to load the annotator and populate
//  the entry.
// TODO - still need a strategy for dealing with persistent errors.
type AnnotatorCache struct {
	// Lock - any thread reading `annotators` should RLock()
	// Any thread writing `annotators` should Lock()
	lock sync.RWMutex

	// Keys are date strings in YYYYMMDD format.
	annotators map[string]*cacheEntry

	numPending int // Number of pending loads.

	// These are static and can be accessed without holding the lock
	loader         func(string) (api.Annotator, error)
	maxEntries     int
	maxPending     int
	minEvictionAge time.Duration // Minimum unused period before eviction
}

// NewAnnotatorCache creates a new map that will use the provided loader for loading new Annotators.
func NewAnnotatorCache(maxEntries int, maxPending int, minAge time.Duration, loader func(string) (api.Annotator, error)) *AnnotatorCache {
	return &AnnotatorCache{annotators: make(map[string]*cacheEntry, 20), loader: loader,
		maxEntries: maxEntries, maxPending: maxPending, minEvictionAge: minAge}
}

// NOTE: Should only be called by loadAnnotator.
// The calling goroutine should "own" the responsibility for
// setting the annotator.
// TODO Add unit tests for load error cases.
func (am *AnnotatorCache) validateAndSetAnnotator(dateString string, ann api.Annotator, err error) error {
	// This prevents readers from trying to read structures while they
	// are being updated.
	am.lock.Lock()

	entry, ok := am.annotators[dateString]
	if !ok || entry == nil {
		am.lock.Unlock()
		log.Println("This should never happen", ErrGoroutineNotOwner)
		return ErrGoroutineNotOwner
	}
	if entry.ann != nil {
		am.lock.Unlock()
		log.Println("This should never happen", ErrMapEntryAlreadySet)
		return ErrMapEntryAlreadySet
	}
	entry.ann = ann
	entry.err = err
	entry.updateLastUsed()
	am.lock.Unlock()

	metrics.LoadCount.Inc()
	log.Println("total", len(am.annotators))
	metrics.PendingLoads.Dec()
	metrics.DatasetCount.Inc()
	am.numPending--
	log.Println("Loaded", dateString)

	return nil
}

// This loads and saves the new dataset.
// It should only be called asynchronously from GetAnnotator.
func (am *AnnotatorCache) loadAnnotator(dateString string) {
	ann, err := am.loader(dateString)
	errorMetricWithLabel(err)
	// Set the new annotator value.  Entry should be nil.
	err = am.validateAndSetAnnotator(dateString, ann, err)
	errorMetricWithLabel(err)
}

// Try loading with EITHER try to evict, OR try to load, OR yield to another thread already loading.
func (am *AnnotatorCache) tryLoading(fn string) {
	am.lock.Lock()
	defer am.lock.Unlock()
	_, ok := am.annotators[fn]
	if ok {
		return
	}
	if am.numPending >= am.maxPending {
		return
	}
	if len(am.annotators) >= am.maxEntries {
		go am.tryEvictOldest()
		return
	}
	// There is no entry yet for this date, so we take ownership by
	// creating an entry.
	am.annotators[fn] = &cacheEntry{}
	metrics.PendingLoads.Inc()
	am.numPending++
	go am.loadAnnotator(fn)
}

// GetAnnotator gets the named annotator, if already in the map.
// If not already loaded, this will trigger loading, and return ErrPendingAnnotatorLoad.
// However, if eviction is required, this will synchronously attempt eviction, and return
// ErrPendingAnnotatorLoad if successful, or ErrAnnotatorLoadFailed if eviction was unsuccessful.
func (am *AnnotatorCache) GetAnnotator(filename string) (api.Annotator, error) {
	am.lock.RLock()
	defer am.lock.RUnlock()
	entry, ok := am.annotators[filename]
	if !ok {
		go am.tryLoading(filename)
		metrics.RejectionCount.WithLabelValues("New Dataset").Inc()
		return nil, ErrPendingAnnotatorLoad
	}

	if entry == nil {
		return nil, ErrNilEntry
	}
	// Update the LRU time.
	entry.updateLastUsed()

	if entry.err != nil {
		return nil, entry.err
	}
	if entry.ann == nil {
		// Another goroutine is already loading this entry.  Return error.
		metrics.RejectionCount.WithLabelValues("New Dataset").Inc()
		return nil, ErrPendingAnnotatorLoad
	}

	return entry.ann, nil
}

func (am *AnnotatorCache) findOldestEntryKey() string {
	am.lock.RLock()
	defer am.lock.RUnlock()
	oldestUsed := time.Now()
	oldestKey := ""
	for k, v := range am.annotators {
		lastUsed := v.getLastUsed()
		if lastUsed.Before(oldestUsed) {
			oldestUsed = lastUsed
			oldestKey = k
		}
	}
	return oldestKey
}

// tryEvictOldest evicts the oldest entry, IFF it has not been referenced in minEvictionAge
// Caller must hold the lock.
func (am *AnnotatorCache) tryEvictOldest() bool {
	oldest := am.findOldestEntryKey()
	am.lock.Lock() // Prevent others from reading.
	defer am.lock.Unlock()

	e, ok := am.annotators[oldest]
	if !ok {
		return false
	}
	if e == nil {
		return false
	}
	if e.err != nil {
		// TODO handle this case better.
		return false
	}
	if e.ann == nil {
		return false
	}

	age := time.Since(e.getLastUsed())
	if age < am.minEvictionAge {
		// TODO add metric for failed eviction requests.
		return false
	}
	log.Println("evicting", age, oldest)
	delete(am.annotators, oldest)
	metrics.EvictionCount.Inc()
	metrics.DatasetCount.Dec()
	return true
}

var lastLog = time.Time{}

// GetAnnotator gets the current annotator.
func GetAnnotator(date time.Time) (api.Annotator, error) {
	filename := geoloader.BestAnnotatorName(date)
	if filename == "" {
		err := errors.New("No Appropriate Dataset")
		errorMetricWithLabel(err)
		return nil, err
	}

	ann, err := allAnnotators.GetAnnotator(filename)
	errorMetricWithLabel(err)
	if time.Since(lastLog) > 5*time.Minute && ann != nil {
		lastLog = time.Now()
		log.Println("Using", ann.AnnotatorDate().Format("20060102"), err, "for", date.Format("20060102"))
	}
	return ann, err
}

// InitAnnotatorCache initializes the allAnnotators cache (if not already initialized)
// Initialized allAnnotators if not already initialized.
func InitAnnotatorCache() {
	if allAnnotators == nil {
		allAnnotators = NewAnnotatorCache(6, 2, 5*time.Minute, geoloader.ArchivedLoader)
	}
}
