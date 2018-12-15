// Package geoloader provides the interface between manager and dataset handling
// packages (geolite2 and legacy). manager only depends on geoloader and api.
// geoloader only depends on geolite2, legacy and api.
package geoloader

import (
	"errors"
	"log"
	"sort"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
)

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

// SelectArchivedDataset returns the archived GeoLite dataset filename given a date.
// For any input date earlier than 2013/08/28, we will return 2013/08/28 dataset.
// For any input date later than latest available dataset, we will return the latest dataset
// Otherwise, we return the last dataset before the input date.
func SelectArchivedDataset(requestDate time.Time) (string, error) {
	if requestDate.Before(EarliestArchiveDate) {
		return "Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz", nil
	}
	lastFilename := ""
	keys := make([]string, 0, len(DatasetFilenames))
	for k := range DatasetFilenames {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	dateString := requestDate.Format("20060102")
	for _, date := range keys {
		// return the last dataset that is earlier than requestDate
		if date > dateString {
			return lastFilename, nil
		}
		lastFilename = DatasetFilenames[date]
	}

	// If there is no filename selected, return the latest dataset
	if lastFilename == "" {
		return "", errors.New("cannot find proper dataset")
	}
	return lastFilename, nil
}

func Geolite2Loader(filename string) (api.Annotator, error) {
	return geolite2.LoadGeoLite2Dataset(filename, api.MaxmindBucketName)
}
