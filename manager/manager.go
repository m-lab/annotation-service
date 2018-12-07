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

	// A mutex to make sure that we are not reading from the CurrentAnnotator
	// pointer while trying to update it
	currentDataMutex = &sync.RWMutex{}

	// CurrentAnnotator points to a GeoDataset struct containing the absolute
	// latest data for the annotator to search and reply with
	CurrentAnnotator api.Annotator
)

// AnnotatorMap manages all loading and fetching of Annotators.
// TODO - should we call this AnnotatorCache?
// TODO - should this be a generic cache of interface{}?
type AnnotatorMap struct {
	// Keys are date strings in YYYYMMDD format.
	annotators map[string]api.Annotator
	// Lock to be held when reading or writing the map.
	mutex  sync.RWMutex
	loader func(string) (api.Annotator, error)
}

// NewAnnotatorMap creates a new map that will use the provided loader for loading new Annotators.
func NewAnnotatorMap(loader func(string) (api.Annotator, error)) *AnnotatorMap {
	return &AnnotatorMap{annotators: make(map[string]api.Annotator, 10), loader: loader}
}

// NOTE: Should only be called by checkAndLoadAnnotator.
// Loads an annotator, and updates the pending map entry.
// On entry, the calling goroutine should "own" the
func (am *AnnotatorMap) loadAnnotator(dateString string) {
	// On entry, this goroutine has exclusive ownership of the
	// map entry, and the responsibility for loading the annotator.
	newAnn, err := am.loader(dateString)
	if err != nil {
		// TODO add a metric
		log.Println(err)
		return
	}

	am.mutex.Lock()
	defer am.mutex.Unlock()

	ann, ok := am.annotators[dateString]
	if !ok {
		// TODO add a metric
		// TODO handle error
	}
	if ann != nil {
		// TODO add a metric
		// TODO handle error
	}
	// TODO add a metric
	am.annotators[dateString] = newAnn
}

// This asynchronously attempts to set map entry to nil, and
// if successful, proceeds to asynchronously load the new dataset.
func (am *AnnotatorMap) checkAndLoadAnnotator(dateString string) {
	go func() {
		am.mutex.Lock()

		_, ok := am.annotators[dateString]
		if ok {
			// Another goroutine is already responsible for loading.
			am.mutex.Unlock()
			return
		}

		// Place marker so that other requesters know it is loading.
		am.annotators[dateString] = nil
		// Drop the lock before attempting to load the annotator.
		am.mutex.Unlock()
		am.loadAnnotator(dateString)
	}()
}

// GetAnnotator gets the named annotator, if already in the map.
// If not already loaded, this will trigger loading, and return ErrPendingAnnotatorLoad
func (am *AnnotatorMap) GetAnnotator(dateString string) (api.Annotator, error) {
	am.mutex.RLock()
	defer am.mutex.RUnlock()

	ann, ok := am.annotators[dateString]
	if !ok {
		am.checkAndLoadAnnotator(dateString)
		return nil, ErrPendingAnnotatorLoad
	}
	if ann == nil {
		return nil, ErrPendingAnnotatorLoad
	}
	return ann, nil
}

// GetAnnotator returns the correct annotator to use for a given timestamp.
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
