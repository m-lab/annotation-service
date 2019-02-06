// Package geoloader provides the interface between manager and dataset handling
// packages (geolite2 and legacy). manager only depends on geoloader and api.
// geoloader only depends on geolite2, legacy and api.
package geoloader

import (
	"context"
	"errors"
	"log"
	"regexp"
	"runtime"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/legacy"
	"github.com/m-lab/annotation-service/metrics"
	"google.golang.org/api/iterator"
)

var (
	// ErrNoLoader is returned if nil is passed for loader parameter.
	ErrNoLoader = errors.New("No loader provided")

	// ErrAfterLegacyCutoff is returned for legacy files beyond the cutoff date.
	errAfterLegacyCutoff = errors.New("After cutoff date")
	// ErrNoMatch is returned (internally) when filename does not match regexp.
	errNoMatch = errors.New("Doesn't match") // TODO
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

func ArchivedLoader(filename string) (api.Annotator, error) {
	if GeoLite2Regex.MatchString(filename) {
		return geolite2.LoadGeoLite2Dataset(filename, api.MaxmindBucketName)
	} else {
		return legacy.LoadLegacyDataset(filename, api.MaxmindBucketName)
	}
}

/*****************************************************************************
*                          LoadAll... functions                              *
*****************************************************************************/

// Returns the normal iterator for objects in the appropriate GCS bucket.
func bucketIterator() (*storage.ObjectIterator, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	prospectiveFiles := client.Bucket(api.MaxmindBucketName).Objects(ctx, &storage.Query{Prefix: api.MaxmindPrefix})
	return prospectiveFiles, nil
}

// LoadAll loads all datasets from the source that match the filter.
func LoadAll(
	filter func(file *storage.ObjectAttrs) error,
	loader func(*storage.ObjectAttrs) (api.Annotator, error)) ([]api.Annotator, error) {
	if loader == nil {
		return nil, ErrNoLoader
	}
	source, err := bucketIterator()
	if err != nil {
		return nil, err
	}

	// TODO - maybe use a channel and single threaded append instead.
	result := make([]api.Annotator, 0, 100)
	resultLock := sync.Mutex{}
	wg := sync.WaitGroup{}

	for file, err := source.Next(); err != iterator.Done; file, err = source.Next() {
		// TODO - should we retry here?
		if err != nil {
			return nil, err
		}
		if file == nil {
			log.Println("file is nil", err)
			continue
		}
		if filter != nil && filter(file) != nil {
			continue
		}
		_, _, callerLine, _ := runtime.Caller(1)
		log.Println("Loading", file.Name, "from line", callerLine)
		wg.Add(1)
		go func(file *storage.ObjectAttrs) {
			defer wg.Done()
			ann, err := loader(file)
			if err != nil {
				log.Println("Retrying", file.Name, "after", err)
				ann, err = loader(file)
				if err != nil {
					log.Println("Failed trying to load", file.Name, "with", err)
					return
				}
			}
			resultLock.Lock()
			result = append(result, ann)
			resultLock.Unlock()
			metrics.DatasetCount.Inc()
			log.Println("Loaded", file.Name)
		}(file)
	}
	wg.Wait()
	return result, nil
}

// filter is used to create filter functions for the loaders.
func filter(file *storage.ObjectAttrs, r *regexp.Regexp, before time.Time) error {
	if !before.Equal(time.Time{}) {
		fileDate, err := api.ExtractDateFromFilename(file.Name)
		if err != nil {
			return err
		}
		if !fileDate.Before(before) {
			return errAfterLegacyCutoff
		}
	}

	if !r.MatchString(file.Name) {
		return errNoMatch
	}

	return nil
}

// LoadAllLegacyV4 loads all v4 legacy datasets from the appropriate GCS bucket.
// The loader is injected, to allow for efficient unit testing.
func LoadAllLegacyV4(loader func(*storage.ObjectAttrs) (api.Annotator, error)) ([]api.Annotator, error) {
	return LoadAll(
		func(file *storage.ObjectAttrs) error {
			return filter(file, GeoLegacyRegex, GeoLite2StartDate)
		},
		loader)
}

// LoadAllLegacyV6 loads all v6 legacy datasets from the appropriate GCS bucket.
// The loader is injected, to allow for efficient unit testing.
func LoadAllLegacyV6(loader func(*storage.ObjectAttrs) (api.Annotator, error)) ([]api.Annotator, error) {
	return LoadAll(
		func(file *storage.ObjectAttrs) error {
			return filter(file, GeoLegacyv6Regex, GeoLite2StartDate)
		},
		loader)
}

// LoadGeolite2 loads a dataset from a GCS object.
func LoadGeolite2(file *storage.ObjectAttrs) (api.Annotator, error) {
	return geolite2.LoadGeoLite2Dataset(file.Name, file.Bucket)
}

// LoadAllGeolite2 loads all geolite2 datasets from the appropriate GCS bucket.
// The loader is injected, to allow for efficient unit testing.
func LoadAllGeolite2(loader func(*storage.ObjectAttrs) (api.Annotator, error)) ([]api.Annotator, error) {
	return LoadAll(
		func(file *storage.ObjectAttrs) error {
			return filter(file, GeoLite2Regex, time.Time{})
		},
		loader)
}
