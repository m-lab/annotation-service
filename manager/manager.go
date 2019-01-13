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

	// ErrTooManyLoading is returned when a new annotator is requested, but not yet loaded.
	ErrTooManyLoading = newCacheError("too many annotators already loading")

	// ErrAnnotatorCacheFull is returned when a new annotator is requested, but cache is full.
	ErrAnnotatorCacheFull = newCacheError("annotator cache is full")

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

func errorMetricWithLabel(err error) bool {
	if err != nil {
		_, _, line, _ := runtime.Caller(1)
		label := fmt.Sprintf("%3d %s", line+2, err)
		metrics.ErrorTotal.WithLabelValues(label).Inc()
		return true
	}
	return false
}

// NOT THREADSAFE.  Should be called once at initialization time.
func SetAnnotatorCacheForTest(ac *AnnotatorCache) {
	if allAnnotators != nil {
		allAnnotators.Cleanup()
	}
	allAnnotators = ac
}

type annotatorEntry struct {
	// date and filenames are immutable.
	date      time.Time // The date associated with this annotator.
	filenames []string  // All filenames associated with this date/annotator.

	lock sync.RWMutex  // Should be held when reading or modifying the annotator pointer or err fields
	ann  api.Annotator // Updated only while holding write lock.

	// UPDATED ATOMICALLY (read or write)
	lastUsed unsafe.Pointer // pointer to the lastUsed time.Time.
	err      CacheError     // Non-nil if there is a non-recoverable error associated with this annotator.
}

type Config struct {
	// These are static and can be accessed without holding the lock
	loader         func(string) (api.Annotator, error)
	maxEntries     int
	maxPending     int
	minEvictionAge time.Duration // Minimum unused period before eviction
}

// directory maintains a list of datasets.
// The map, slice, and cacheConfig are immutable once initialized.
type directory struct {
	cacheConfig struct{}                   // Config information for cache
	entries     map[string]*annotatorEntry // Map from dateStrings to annotatorEntries.
	dates       []string                   // Ordered list for sort.Search

	evictionTerminate chan struct{} // Channel for killing the eviction timer.
}

// TODO - add a timer.AfterFunc() to automatically evict after
// some time limit, perhaps 2x the minAge?
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
	if ptr == nil {
		log.Println("Error in getLastUsed for", ce)
		return time.Time{}
	}
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
	config Config
	// Lock - any thread reading `annotators` should RLock()
	// Any thread writing `annotators` should Lock()
	lock sync.RWMutex

	// Keys are date strings in YYYYMMDD format.
	annotators map[string]*cacheEntry

	pendingTokens chan struct{} // Used to limit the number of concurrent loads.
	limitTokens   chan struct{} // Used to limit the number of loaded datasets.

	evictionTerminate chan struct{} // Channel for killing the eviction timer.
}

func (ac *AnnotatorCache) releasePending() {
	<-ac.pendingTokens
}

func (ac *AnnotatorCache) releaseLimit() {
	<-ac.limitTokens
}

// Returns nil if successful, otherwise failure error.
func (ac *AnnotatorCache) tryGetToken() error {
	select {
	case ac.pendingTokens <- struct{}{}:
	default:
		return ErrTooManyLoading
	}

	// Now we have the pending load token, try to get the maxEntries token
	select {
	case ac.limitTokens <- struct{}{}:
		return nil
	default:
		<-ac.pendingTokens // Failed, so return the pending token.
		return ErrAnnotatorCacheFull
	}
}

// NewAnnotatorCache creates a new map that will use the provided loader for loading new Annotators.
// It also starts a timer that will trigger evictions 5 times per minAge interval.
func NewAnnotatorCache(maxEntries int, maxPending int, minAge time.Duration, loader func(string) (api.Annotator, error)) *AnnotatorCache {
	config := Config{loader: loader, maxEntries: maxEntries, maxPending: maxPending, minEvictionAge: minAge}
	ac := AnnotatorCache{annotators: make(map[string]*cacheEntry, 20), config: config, pendingTokens: make(chan struct{}, maxPending),
		limitTokens: make(chan struct{}, maxEntries)}
	ac.evictEvery(minAge / 5)
	return &ac
}

// NOTE: Should only be called by loadAnnotator.
// The calling goroutine should "own" the responsibility for
// setting the annotator.
// TODO Add unit tests for load error cases.
func (am *AnnotatorCache) validateAndSetAnnotator(fn string, ann api.Annotator, err error) error {
	// This prevents readers from trying to read structures while they
	// are being updated.
	am.lock.Lock()

	entry, ok := am.annotators[fn]
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

	// HACK for now
	// TODO - test this functionality
	if err != nil {
		metrics.ErrorTotal.WithLabelValues("load failed").Inc()
		log.Println("Loading failed.  Hack for now - deleting entry")
		// HACK so this doesn't take up a slot.
		am.releaseLimit()
	} else {
		metrics.DatasetCount.Inc()
	}

	entry.ann = ann
	entry.err = err
	entry.updateLastUsed()
	total := len(am.annotators)
	am.lock.Unlock()

	metrics.LoadCount.Inc()
	log.Println("total", total)
	metrics.PendingLoads.Dec()
	log.Println("Loaded", fn)

	return nil
}

// This loads and saves the new dataset.
// It should only be called asynchronously from GetAnnotator.
func (am *AnnotatorCache) loadAnnotator(fn string) {
	defer am.releasePending() // Release the token when loading completes or fails.
	ann, err := am.config.loader(fn)
	if errorMetricWithLabel(err) {
		log.Println("Loading error", err, fn)
	}
	// Set the new annotator value.  Entry should be nil.
	err = am.validateAndSetAnnotator(fn, ann, err)
	if errorMetricWithLabel(err) {
		log.Println("Loading error", err, fn)
	}
}

// Kick off loading, OR yield to another thread already loading, OR return error.
func (am *AnnotatorCache) tryLoading(fn string) error {
	err := am.tryGetToken()
	if err != nil {
		return err
	}

	go func() {
		am.lock.Lock()
		defer am.lock.Unlock()
		_, ok := am.annotators[fn]
		if ok {
			// Another thread won the race.
			am.releaseLimit()
			am.releasePending()
			return
		}

		// There is no entry yet for this date, so we take ownership by
		// creating an entry.
		am.annotators[fn] = &cacheEntry{} // Note: this changes the "total" logs.
		metrics.PendingLoads.Inc()
		// Implicitly pass the semaphore to the loader.
		go am.loadAnnotator(fn)
		log.Println("Loading", fn, "asynchronously")
	}()
	return nil
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
		metrics.RejectionCount.WithLabelValues("New Dataset").Inc()
		loadErr := am.tryLoading(filename)
		if loadErr != nil {
			return nil, loadErr
		}
		return nil, ErrPendingAnnotatorLoad
	}

	if entry == nil {
		return nil, ErrNilEntry
	}
	// Update the LRU time.
	entry.updateLastUsed()

	if entry.err != nil {
		metrics.RejectionCount.WithLabelValues("Permanent loading error").Inc()
		return nil, entry.err
	}
	if entry.ann == nil {
		// Another goroutine is already loading this entry.  Return error.
		metrics.RejectionCount.WithLabelValues("New Dataset").Inc()
		return nil, ErrPendingAnnotatorLoad
	}

	return entry.ann, nil
}

func (am *AnnotatorCache) findEvictionCandidates() []string {
	candidates := make([]string, 3)
	am.lock.RLock()
	defer am.lock.RUnlock()
	for k, v := range am.annotators {
		lastUsed := v.getLastUsed()
		if time.Since(lastUsed) > am.config.minEvictionAge {
			candidates = append(candidates, k)
		}
	}
	return candidates
}

// evictExpired evicts any entry that has not been referenced in minEvictionAge
func (am *AnnotatorCache) evictExpired() {
	candidates := am.findEvictionCandidates()
	if len(candidates) <= 0 {
		return
	}

	for _, c := range candidates {
		am.lock.Lock() // Prevent others from reading.
		e, ok := am.annotators[c]
		if !ok || e == nil || e.err != nil || e.ann == nil {
			// TODO log an error
			am.lock.Unlock()
			continue
		}
		age := time.Since(e.getLastUsed())
		if age < am.config.minEvictionAge {
			am.lock.Unlock()
			continue
		}
		log.Println("evicting", age, c)
		// Note that this may block if legacy dataset is currently in use by other threads.
		am.annotators[c].ann.Close()
		delete(am.annotators, c)
		log.Println("total datasets", len(am.annotators))
		am.lock.Unlock()

		am.releaseLimit() // Allow additional dataset to be loaded.
		metrics.EvictionCount.Inc()
		metrics.DatasetCount.Dec()
	}
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
	if time.Since(lastLog) > 5*time.Minute && ann != nil {
		lastLog = time.Now()
		log.Println("Using", ann.AnnotatorDate().Format("20060102"), err, "for", date.Format("20060102"))
	}
	errorMetricWithLabel(err)
	if err == nil || err == ErrAnnotatorCacheFull || err == ErrPendingAnnotatorLoad || err == ErrTooManyLoading {
		return ann, err
	}
	// Try an earlier annotator...
	// Found that 2014/01/07 fails to load, so we need to deal with it.
	// TODO test this functionality
	log.Println("Substituting an earlier annotator")
	return GetAnnotator(date.Add(-30 * 24 * time.Hour))
}

func (am *AnnotatorCache) evictEvery(interval time.Duration) {
	if am.evictionTerminate != nil {
		am.evictionTerminate <- struct{}{}
	}
	am.evictionTerminate = make(chan struct{}, 0)
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-am.evictionTerminate:
				ticker.Stop()
				return
			case <-ticker.C:
				am.evictExpired()
			}
		}
	}()
}

func (am *AnnotatorCache) Cleanup() {
	am.evictionTerminate <- struct{}{}
}

// InitAnnotatorCache initializes the allAnnotators cache (if not already initialized)
// Initialized allAnnotators if not already initialized.
func InitAnnotatorCache() {
	if allAnnotators == nil {
		allAnnotators = NewAnnotatorCache(14, 2, 5*time.Minute, geoloader.ArchivedLoader)
	}
}
