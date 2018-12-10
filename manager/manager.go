package manager

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
)

var (
	// ErrNilDataset is returned when CurrentAnnotator is nil.
	ErrNilDataset = errors.New("CurrentAnnotator is nil")

	// ErrPendingAnnotatorLoad is returned when a new annotator is requested, but not yet loaded.
	ErrPendingAnnotatorLoad = errors.New("annotator is loading")

	// ErrAnnotatorLoadFailed is returned when a requested annotator has failed to load.
	ErrAnnotatorLoadFailed = errors.New("unable to load annoator")

	// These are UNEXPECTED errors!!
	// ErrGoroutineNotOwner is returned when goroutine attempts to set annotator entry, but is not the owner.
	ErrGoroutineNotOwner = errors.New("goroutine not owner")
	// ErrMapEntryAlreadySet is returned when goroutine attempts to set annotator, but entry is non-null.
	ErrMapEntryAlreadySet = errors.New("map entry already set")
	// ErrNilEntry is returned when map has a nil entry, which should never happen.
	ErrNilEntry = errors.New("Map entry is nil")

	// A mutex to make sure that we are not reading from the CurrentAnnotator
	// pointer while trying to update it
	currentDataMutex = &sync.RWMutex{}

	// CurrentAnnotator points to a GeoDataset struct containing the absolute
	// latest data for the annotator to search and reply with
	CurrentAnnotator api.Annotator
)

type cacheEntry struct {
	ann      api.Annotator
	lastUsed time.Time
}

// AnnotatorMap manages all loading and fetching of Annotators.
// TODO - should we call this AnnotatorCache?
// TODO - should this be a generic cache of interface{}?
//
// Synchronization:
//  All accesses must hold the mutex.  If an element is not found, the
//  goroutine may attempt to take responsibility for loading it by obtaining
//  the write lock, and writing an entry with a nil pointer.
// TODO - still need a strategy for dealing with persistent errors.
type AnnotatorMap struct {
	// Keys are date strings in YYYYMMDD format.
	annotators map[string]*cacheEntry
	// Lock to be held when reading or writing the map.
	mutex       sync.Mutex
	oldestIndex string // The index of the oldest entry.
	loader      func(string) (api.Annotator, error)
}

// NewAnnotatorMap creates a new map that will use the provided loader for loading new Annotators.
func NewAnnotatorMap(loader func(string) (api.Annotator, error)) *AnnotatorMap {
	return &AnnotatorMap{annotators: make(map[string]*cacheEntry, 10), loader: loader}
}

// NOTE: Should only be called by checkAndLoadAnnotator.
// The calling goroutine should "own" the responsibility for
// setting the annotator.
func (am *AnnotatorMap) validateAndSetAnnotator(dateString string, ann api.Annotator) error {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	entry, ok := am.annotators[dateString]
	if !ok || entry == nil {
		return ErrGoroutineNotOwner
	}
	if entry.ann != nil {
		return ErrMapEntryAlreadySet
	}
	entry.ann = ann
	entry.lastUsed = time.Now()
	return nil
}

// This loads and saves the new dataset.
// It should only be called asynchronously from GetAnnotator.
func (am *AnnotatorMap) loadAnnotator(dateString string) {
	// This goroutine now has exclusive ownership of the
	// map entry, and the responsibility for loading the annotator.
	go func(dateString string) {
		ann, err := am.loader(dateString)
		if err != nil {
			// TODO add a metric
			log.Println(err)
			return
		}
		// Set the new annotator value.  Entry should be nil.
		err = am.validateAndSetAnnotator(dateString, ann)
		if err != nil {
			// TODO add a metric
			log.Println(err)
		}
	}(dateString)
}

func (am *AnnotatorMap) updateOldest() {
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
// If not already loaded, this will trigger loading, and return ErrPendingAnnotatorLoad
func (am *AnnotatorMap) GetAnnotator(dateString string) (api.Annotator, error) {
	am.mutex.Lock()
	defer am.mutex.Unlock()
	entry, ok := am.annotators[dateString]
	if !ok {
		// There is no entry yet for this date, so we take ownership by
		// creating an entry.
		am.annotators[dateString] = &cacheEntry{}
		go am.loadAnnotator(dateString)
		// Another goroutine is already loading this entry.  Return error.
		return nil, ErrPendingAnnotatorLoad
	}

	// TODO check for nil entry??
	if entry == nil {
		return nil, ErrNilEntry
	} else if entry.ann == nil {
		return nil, ErrPendingAnnotatorLoad
	}
	// Update the LRU time.
	entry.lastUsed = time.Now()
	if am.oldestIndex == dateString {
		am.updateOldest()
	}

	return entry.ann, nil
}

// EvictOneOlderThan evicts the oldest entry, IFF it was last referenced no later than `t`
func (am *AnnotatorMap) EvictOneOlderThan(t time.Time) api.Annotator {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	if am.oldestIndex == "" {
		return nil
	}

	e := am.annotators[am.oldestIndex]
	if e.lastUsed.Before(t) {
		delete(am.annotators, am.oldestIndex)
		am.updateOldest()
	}
	return nil
}

// GetAnnotator gets the current annotator.
func GetAnnotator(date time.Time) api.Annotator {
	// TODO - use the requested date
	// dateString := strconv.FormatInt(date.Unix(), encodingBase)
	currentDataMutex.RLock()
	ann := CurrentAnnotator
	currentDataMutex.RUnlock()
	return ann
}

// PopulateLatestData will search to the latest Geolite2 files
// available in GCS and will use them to create a new GeoDataset which
// it will place into the global scope as the latest version. It will
// do so safely with use of the currentDataMutex RW mutex. It it
// encounters an error, it will halt the program.
func PopulateLatestData() {
	data, err := geolite2.LoadLatestGeolite2File()
	if err != nil {
		log.Fatal(err)
	}
	currentDataMutex.Lock()
	CurrentAnnotator = data
	currentDataMutex.Unlock()
}
