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
	ArchivedLoader AnnotatorLoader

	// ErrPendingAnnotatorLoad is returned when a new annotator is requested, but not yet loaded.
	ErrPendingAnnotatorLoad = errors.New("annotator is loading")

	ErrAnnotatorLoadFailed = errors.New("unable to load annoator")
)

// AnnotatorLoader manages all loading of and already loaded Annotators
type AnnotatorLoader struct {
	// Keys are date strings in YYYYMMDD format.
	annotators map[string]api.Annotator
	// Lock to be held when reading or writing the map.
	mutex sync.RWMutex
}

// NOTE: Should only be called by checkAndLoadAnnotator.
// Loads an annotator, and updates the pending map entry.
// On entry, the calling goroutine should "own" the
func (am *AnnotatorLoader) loadAnnotator(dateString string) {
	// On entry, this goroutine has exclusive ownership of the
	// map entry, and the responsibility for loading the annotator.
	var ann api.Annotator = nil
	// TODO actually load the annotator and handle loading errors.

	am.mutex.Lock()
	defer am.mutex.Unlock()

	ann, ok := am.annotators[dateString]
	if !ok {
		// TODO handle error
	}
	if ann != nil {
		// TODO handle error
	}
	am.annotators[dateString] = ann
}

// This asynchronously attempts to set map entry to nil, and
// if successful, proceeds to asynchronously load the new dataset.
func (am *AnnotatorLoader) checkAndLoadAnnotator(dateString string) {
	go func() {
		am.mutex.Lock()

		_, ok := am.annotators[dateString]
		if ok {
			// Another goroutine is already responsible for loading.
			am.mutex.Unlock()
			return
		} else {
			// Place marker so that other requesters know it is loading.
			am.annotators[dateString] = nil
		}

		// Drop the lock before attempting to load the annotator.
		am.mutex.Unlock()
		am.loadAnnotator(dateString)
	}()
}

// Gets the named annotator, if already in the map.
func (am *AnnotatorLoader) GetAnnotator(dateString string) (api.Annotator, error) {
	am.mutex.RLock()
	defer am.mutex.RUnlock()

	ann, ok := am.annotators[dateString]
	if ok {
		return ann, nil
	} else {
		am.checkAndLoadAnnotator(dateString)
		return nil, ErrPendingAnnotatorLoad
	}
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
