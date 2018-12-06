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
	// GeoLite2Annotator points to Geolite2 datasets in memory.
	Geolite2Annotator map[string]api.Annotator

	// A mutex to make sure that we are not reading from the Geolite2Annotator
	// pointer while trying to update it
	archivedDataMutex = &sync.RWMutex{}
)

// GetArchivedAnnotator returns the pointer to the dataset in memory with the filename.
func GetArchivedAnnotator(filename string) api.Annotator {
	ann := Geolite2Annotator[filename]
	if ann != nil {
		return ann
	}

	// load new dataset into memory if it is not there already
	archivedDataMutex.Lock()
	if len(Geolite2Annotator) >= MaxHistoricalGeolite2Dataset {
		// Remove one entry
		for key, _ := range Geolite2Annotator {
			log.Println("remove Geolite2 dataset " + key)
			delete(Geolite2Annotator, key)
			break
		}
	}
	ann, err := geolite2.LoadGeoLite2Dataset(filename, api.MaxmindBucketName)
	if err != nil {
		return nil
	}
	log.Println("historical Geolite2 dataset loaded " + filename)
	Geolite2Annotator[filename] = ann
	log.Printf("number of Geolite2 dataset in memory: %d ", len(Geolite2Annotator))
	log.Println(Geolite2Annotator)
	archivedDataMutex.Unlock()

	return ann
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
