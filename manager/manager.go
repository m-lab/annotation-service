// Package manager provides interface between handler and lower level implementation
// such as geoloader.
package manager

// The implementation is currently rather naive.  Eviction is done based only on whether
// there is a pending request, and there are already the max number of datasets loaded.
// A later implementation will use LRU and dead time to make this determination.

// Please modify with extreme caution.  The lock MUST be held when ACCESSING any field
// of AnnotatorMap.

// Note that the system may evict up to the number of pending loads, so at any given time,
// there may only be MaxDatasetInMemory = MaxPending actually loaded.

// Also note that anyone holding an annotator will prevent it from being collected by the
// GC, so simply evicting it is not a guarantee that the memory will be reclaimed.

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geoloader"
)

var (
	// These are vars instead of consts to facilitate testing.
	MaxDatasetInMemory = 5 // Limit on number of loaded datasets
	MaxPending         = 2 // Limit on number of concurrently loading datasets.

	// ErrNilDataset is returned when CurrentAnnotator is nil.
	ErrNilDataset = errors.New("CurrentAnnotator is nil")

	// ErrPendingAnnotatorLoad is returned when a new annotator is requested, but not yet loaded.
	ErrPendingAnnotatorLoad = errors.New("annotator is loading")

	// ErrAnnotatorLoadFailed is returned when a requested annotator has failed to load.
	ErrAnnotatorLoadFailed = errors.New("unable to load annoator")

	// These are UNEXPECTED errors!!
	ErrGoroutineNotOwner  = errors.New("Goroutine not owner")
	ErrMapEntryAlreadySet = errors.New("Map entry already set")

	// A mutex to make sure that we are not reading from the CurrentAnnotator
	// pointer while trying to update it
	currentDataMutex = &sync.RWMutex{}

	// CurrentAnnotator points to a GeoDataset struct containing the absolute
	// latest data for the annotator to search and reply with
	CurrentAnnotator api.Annotator

	// ArchivedLoader points to a AnnotatorMap struct containing the archived
	// Geolite2 dataset in memory.
	archivedAnnotator = NewAnnotatorMap(geoloader.Geolite2Loader)
)

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
	// Keys are filename of the datasets.
	annotators map[string]api.Annotator
	// Lock to be held when reading or writing the map.
	mutex      sync.RWMutex
	numPending int
	loader     func(string) (api.Annotator, error)
}

// NewAnnotatorMap creates a new map that will use the provided loader for loading new Annotators.
func NewAnnotatorMap(loader func(string) (api.Annotator, error)) *AnnotatorMap {
	return &AnnotatorMap{annotators: make(map[string]api.Annotator), loader: loader}
}

// NOTE: Should only be called by checkAndLoadAnnotator.
// The calling goroutine should "own" the responsibility for
// setting the annotator.
func (am *AnnotatorMap) setAnnotatorIfNil(key string, ann api.Annotator) error {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	old, ok := am.annotators[key]
	if !ok {
		log.Println("This should never happen", ErrGoroutineNotOwner)
		return ErrGoroutineNotOwner
	}
	if old != nil {
		log.Println("This should never happen", ErrMapEntryAlreadySet)
		return ErrMapEntryAlreadySet
	}

	am.annotators[key] = ann
	am.numPending--
	log.Println("Loaded", key)
	return nil
}

func (am *AnnotatorMap) maybeSetNil(key string) bool {
	am.mutex.Lock()
	defer am.mutex.Unlock()
	_, ok := am.annotators[key]
	if ok {
		// Another goroutine is already responsible for loading.
		return false
	}

	if am.numPending >= MaxPending {
		log.Println("Too many pending", key)
		return false
	}
	// Check the number of datasets in memory. Given the memory
	// limit, some dataset may be removed from memory if needed.
	if len(am.annotators) >= MaxDatasetInMemory {
		for fileKey := range am.annotators {
			if am.annotators[fileKey] != nil {
				log.Println("removing Geolite2 dataset " + fileKey)
				delete(am.annotators, fileKey)
				break
			}
		}
		return false
	}

	// Place marker so that other requesters know it is loading.
	am.annotators[key] = nil
	am.numPending++
	return true
}

// This synchronously attempts to set map entry to nil, and
// if successful, proceeds to asynchronously load the new dataset.
func (am *AnnotatorMap) checkAndLoadAnnotator(key string) {
	if !geoloader.GeoLite2Regex.MatchString(key) {
		return
	}
	reserved := am.maybeSetNil(key)
	if reserved {
		// This goroutine now has exclusive ownership of the
		// map entry, and the responsibility for loading the annotator.
		go func(key string) {
			// TODO - this is currently redundant, as we already checked this.
			if geoloader.GeoLite2Regex.MatchString(key) {
				log.Println("plan to load " + key)
				newAnn, err := am.loader(key)
				if err != nil {
					// TODO add a metric
					log.Println(err)
					return
				}
				// Set the new annotator value.  Entry should be nil.
				err = am.setAnnotatorIfNil(key, newAnn)
				if err != nil {
					// TODO add a metric
					log.Println(err)
				}
			} else {
				// TODO load legacy binary dataset
				return
			}
		}(key)
	}
}

// GetAnnotator gets the named annotator, if already in the map.
// If not already loaded, this will trigger loading, and return ErrPendingAnnotatorLoad
func (am *AnnotatorMap) GetAnnotator(key string) (api.Annotator, error) {
	am.mutex.RLock()
	ann, ok := am.annotators[key]
	am.mutex.RUnlock()

	if !ok {
		// There is not yet any entry for this date.  Try to load it.
		am.checkAndLoadAnnotator(key)
		return nil, ErrPendingAnnotatorLoad
	}

	if ann == nil {
		// Another goroutine is already loading this entry.  Return error.
		return nil, ErrPendingAnnotatorLoad
	}
	return ann, nil
}

// GetAnnotator returns the correct annotator to use for a given timestamp.
func GetAnnotator(date time.Time) (api.Annotator, error) {
	// key := strconv.FormatInt(date.Unix(), encodingBase)
	if date.After(geoloader.LatestDatasetDate) {
		currentDataMutex.RLock()
		ann := CurrentAnnotator
		currentDataMutex.RUnlock()
		return ann, nil
	}
	if date.Before(geoloader.GeoLite2StartDate) {
		currentDataMutex.RLock()
		ann := CurrentAnnotator
		currentDataMutex.RUnlock()
		return ann, nil
	}
	filename, err := geoloader.SelectArchivedDataset(date)

	if err != nil {
		return nil, err
	}

	return archivedAnnotator.GetAnnotator(filename)
}

// InitDataset will update the filename list of archived dataset in memory
// and load the latest Geolite2 dataset in memory.
func InitDataset() {
	geoloader.UpdateArchivedFilenames()

	currentDataMutex.Lock()
	CurrentAnnotator = geoloader.GetLatestData()
	currentDataMutex.Unlock()
}
