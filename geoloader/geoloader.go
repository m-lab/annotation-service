package geoloader

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
)

const (
	// Maximum number of Geolite2 datasets in memory.
	MaxHistoricalGeolite2Dataset = 5
)

var (
	// ErrPendingAnnotatorLoad is returned when a new annotator is requested, but not yet loaded.
	ErrPendingAnnotatorLoad = errors.New("annotator is loading")

	// ErrAnnotatorLoadFailed is returned when a requested annotator has failed to load.
	ErrAnnotatorLoadFailed = errors.New("unable to load annoator")

	// These are UNEXPECTED errors!!
	ErrGoroutineNotOwner  = errors.New("Goroutine not owner")
	ErrMapEntryAlreadySet = errors.New("Map entry already set")

	ArchivedLoader AnnotatorMap
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
// The calling goroutine should "own" the responsibility for
// setting the annotator.
func (am *AnnotatorMap) setAnnotatorIfNil(dateString string, ann api.Annotator) error {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	old, ok := am.annotators[dateString]
	if !ok {
		return ErrGoroutineNotOwner
	}
	if old != nil {
		return ErrMapEntryAlreadySet
	}
	am.annotators[dateString] = ann
	return nil
}

func (am *AnnotatorMap) maybeSetNil(dateString string) bool {
	am.mutex.Lock()
	defer am.mutex.Unlock()
	_, ok := am.annotators[dateString]
	if ok {
		// Another goroutine is already responsible for loading.
		return false
	}

	// Place marker so that other requesters know it is loading.
	am.annotators[dateString] = nil
	return true
}

// This synchronously attempts to set map entry to nil, and
// if successful, proceeds to asynchronously load the new dataset.
func (am *AnnotatorMap) checkAndLoadAnnotator(dateString string) {
	reserved := am.maybeSetNil(dateString)
	if reserved {
		// This goroutine now has exclusive ownership of the
		// map entry, and the responsibility for loading the annotator.
		go func(dateString string) {
			newAnn, err := am.loader(dateString)
			if err != nil {
				// TODO add a metric
				log.Println(err)
				return
			}
			// Set the new annotator value.  Entry should be nil.
			err = am.setAnnotatorIfNil(dateString, newAnn)
			if err != nil {
				// TODO add a metric
				log.Println(err)
			}
		}(dateString)
	}
}

// GetAnnotator gets the named annotator, if already in the map.
// If not already loaded, this will trigger loading, and return ErrPendingAnnotatorLoad
func (am *AnnotatorMap) GetAnnotator(dateString string) (api.Annotator, error) {
	am.mutex.RLock()
	ann, ok := am.annotators[dateString]
	am.mutex.RUnlock()

	if !ok {
		// There is not yet any entry for this date.  Try to load it.
		am.checkAndLoadAnnotator(dateString)
		return nil, ErrPendingAnnotatorLoad
	}
	if ann == nil {
		// Another goroutine is already loading this entry.  Return error.
		return nil, ErrPendingAnnotatorLoad
	}
	return ann, nil
}

// GetArchivedAnnotator returns the pointer to the dataset in memory with the filename.
func GetArchivedAnnotator(filename string) api.Annotator {
	ann, err := ArchivedLoader.GetAnnotator(filename)

	if err == nil {
		return ann
	}
	return nil
}

// PopulateLatestData will search to the latest Geolite2 files
// available in GCS and will use them to create a new GeoDataset which
// it will place into the global scope as the latest version. It will
// do so safely with use of the currentDataMutex RW mutex. It it
// encounters an error, it will halt the program.
func GetLatestData() api.Annotator {
	data, err := geolite2.LoadLatestGeolite2File()
	if err != nil {
		log.Fatal(err)
	}
	return data
}

// SelectArchivedDataset returns the archived GelLite dataset filename given a date.
// For any input date earlier than 2013/08/28, we will return 2013/08/28 dataset.
// For any input date later than latest available dataset, we will return the latest dataset
// Otherwise, we return the last dataset before the input date.
func SelectArchivedDataset(requestDate time.Time) (string, error) {
	earliestArchiveDate, _ := time.Parse("January 2, 2006", "August 28, 2013")
	if requestDate.Before(earliestArchiveDate) {
		return "Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz", nil
	}
	CutOffDate, _ := time.Parse("January 2, 2006", GeoLite2CutOffDate)
	lastFilename := ""
	for _, fileName := range DatasetNames {
		if requestDate.Before(CutOffDate) && (GeoLegacyRegex.MatchString(fileName) || GeoLegacyv6Regex.MatchString(fileName)) {
			// search legacy dataset
			fileDate, err := ExtractDateFromFilename(fileName)
			if err != nil {
				continue
			}
			// return the last dataset that is earlier than requestDate
			if fileDate.After(requestDate) {
				return lastFilename, nil
			}
			lastFilename = fileName
		} else if !requestDate.Before(CutOffDate) && GeoLite2Regex.MatchString(fileName) {
			// Search GeoLite2 dataset
			fileDate, err := ExtractDateFromFilename(fileName)
			if err != nil {
				continue
			}
			// return the last dataset that is earlier than requestDate
			if fileDate.After(requestDate) {
				return lastFilename, nil
			}
			lastFilename = fileName
		}
	}
	// If there is no filename selected, return the latest dataset
	if lastFilename == "" {
		return "", errors.New("cannot find proper dataset")
	}
	return lastFilename, nil
}
