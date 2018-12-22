package manager

// The AnnotatorCache is an LRU cache that handles loading and eviction of annotators in
// response to requests for specific dates.
// Configuration parameters are:
//   minEvictionAge - the minimum period that an annotator should be idle before eviction.
//   maxPendingLoads - the maximum number of concurrent loads allowed.
//   maxEntries - the maximum number of entries allowed, to avoid OOM problems.
//   loader - the function that handles loading of a new annotator.

import (
	"errors"
	"log"
	"sync"
	"time"

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

// NOT THREADSAFE.  Should be called once at initialization time.
func SetAnnotatorCacheForTest(ac *AnnotatorCache) {
	allAnnotators = ac
}

type cacheEntry struct {
	ann      api.Annotator
	lastUsed time.Time
	//
	err CacheError
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
	// Lock to be held when reading or writing the map or oldestIndex.
	lock sync.Mutex
	// Keys are date strings in YYYYMMDD format.
	annotators  map[string]*cacheEntry
	numPending  int    // Number of pending loads.
	oldestIndex string // The index of the oldest entry.

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
	am.lock.Lock()
	defer am.lock.Unlock()

	entry, ok := am.annotators[dateString]
	if !ok || entry == nil {
		log.Println("This should never happen", ErrGoroutineNotOwner)
		return ErrGoroutineNotOwner
	}
	if entry.ann != nil {
		log.Println("This should never happen", ErrMapEntryAlreadySet)
		return ErrMapEntryAlreadySet
	}
	entry.ann = ann
	entry.err = err
	entry.lastUsed = time.Now()

	metrics.LoadCount.Inc()
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
	if err != nil {
		metrics.ErrorTotal.WithLabelValues(err.Error()).Inc()
	}
	// Set the new annotator value.  Entry should be nil.
	err = am.validateAndSetAnnotator(dateString, ann, err)
	if err != nil {
		metrics.ErrorTotal.WithLabelValues(err.Error()).Inc()
	}
}

// Client must hold write lock.
func (am *AnnotatorCache) updateOldest() {
	am.oldestIndex = ""
	if len(am.annotators) == 0 {
		return
	}
	oldest := time.Now()
	// Scan through the map keys to find the oldest key.
	for k, e := range am.annotators {
		if e.lastUsed.Before(oldest) {
			oldest = e.lastUsed
			am.oldestIndex = k
		}
	}
}

// GetAnnotator gets the named annotator, if already in the map.
// If not already loaded, this will trigger loading, and return ErrPendingAnnotatorLoad.
// However, if eviction is required, this will synchronously attempt eviction, and return
// ErrPendingAnnotatorLoad if successful, or ErrAnnotatorLoadFailed if eviction was unsuccessful.
func (am *AnnotatorCache) GetAnnotator(dateString string) (api.Annotator, error) {
	log.Println("Requested", dateString)
	am.lock.Lock()
	defer am.lock.Unlock()
	entry, ok := am.annotators[dateString]
	if !ok {
		metrics.RejectionCount.WithLabelValues("New Dataset").Inc()
		if am.numPending >= am.maxPending {
			log.Println("Too many loading")
			return nil, ErrPendingAnnotatorLoad
		}
		if len(am.annotators) >= am.maxEntries {
			if !am.tryEvictOldest() {
				return nil, ErrAnnotatorLoadFailed
			}
			return nil, ErrPendingAnnotatorLoad
		}
		// There is no entry yet for this date, so we take ownership by
		// creating an entry.
		am.annotators[dateString] = &cacheEntry{}
		metrics.PendingLoads.Inc()
		am.numPending++
		go am.loadAnnotator(dateString)
		return nil, ErrPendingAnnotatorLoad
	}

	if entry == nil {
		return nil, ErrNilEntry
	} else if entry.err != nil {
		return nil, entry.err
	} else if entry.ann == nil {
		// Another goroutine is already loading this entry.  Return error.
		metrics.RejectionCount.WithLabelValues("New Dataset").Inc()
		return nil, ErrPendingAnnotatorLoad
	}

	// Update the LRU time.
	entry.lastUsed = time.Now()
	if am.oldestIndex == dateString || am.oldestIndex == "" {
		am.updateOldest()
	}

	return entry.ann, nil
}

// tryEvictOldest evicts the oldest entry, IFF it has not been referenced in minEvictionAge
// Caller must hold the lock.
func (am *AnnotatorCache) tryEvictOldest() bool {
	if am.oldestIndex == "" {
		return false
	}

	e := am.annotators[am.oldestIndex]
	if time.Since(e.lastUsed) < am.minEvictionAge {
		// TODO add metric for failed eviction requests.
		return false
	}
	log.Println("evicting", am.oldestIndex)
	delete(am.annotators, am.oldestIndex)
	log.Println("total", len(am.annotators))
	am.updateOldest()
	metrics.EvictionCount.Inc()
	metrics.DatasetCount.Dec()

	return true
}

// GetAnnotator gets the current annotator.
func GetAnnotator(date time.Time) (api.Annotator, error) {
	filename := geoloader.BestAnnotatorName(date)

	if filename == "" {
		metrics.ErrorTotal.WithLabelValues("No Appropriate Dataset").Inc()
		return nil, errors.New("No Appropriate Dataset")
	}

	return allAnnotators.GetAnnotator(filename)
}

// InitDataset will update the filename list of archived dataset in memory
// and load the latest Geolite2 dataset in memory.
// Initialized allAnnotators if not already initialized.
func InitDataset() {
	if allAnnotators == nil {
		allAnnotators = NewAnnotatorCache(12, 1, 5*time.Minute, geoloader.ArchivedLoader)
	}
	geoloader.UpdateArchivedFilenames()

	// TODO - preload the most recent?
}
