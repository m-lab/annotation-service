// Package geoloader provides the interface between manager and dataset handling
// packages (geolite2 and legacy). manager only depends on geoloader and api.
// geoloader only depends on geolite2, legacy and api.
// TODO:  This package is now used only by the manager package.  Should we consolidate them?
package geoloader

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"regexp"
	"runtime"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/metrics"
	"google.golang.org/api/iterator"
)

const (
	// Folder containing the maxmind files
	maxmindPrefix = "Maxmind/"
)

var (
	// geoLite2StartDate is the date we have the first GeoLite2 dataset.
	// Any request earlier than this date using legacy binary datasets
	// later than this date using GeoLite2 datasets
	// TODO make this local
	geoLite2StartDate = time.Unix(1502755200, 0) //"August 15, 2017"

	// geoLite2Regex is used to filter which geolite2 dataset files we consider acceptable.
	geoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)

	// GeoLegacy??Regex are used to filter which legacy dataset files we consider acceptable.
	geoLegacyRegex   = regexp.MustCompile(`.*-GeoLiteCity.dat.*`)
	geoLegacyv6Regex = regexp.MustCompile(`.*-GeoLiteCityv6.dat.*`)

	// ErrNoLoader is returned if nil is passed for loader parameter.
	ErrNoLoader = errors.New("No loader provided")

	// ErrAfterLegacyCutoff is returned for legacy files beyond the cutoff date.
	errAfterLegacyCutoff = errors.New("After cutoff date")
	// ErrNoMatch is returned (internally) when filename does not match regexp.
	errNoMatch = errors.New("Doesn't match") // TODO
)

// UseSpecificGeolite2DateForTesting is for unit tests to narrow the datasets to load from GCS to date that can be matched to the date part regexes.
// The parameters are string pointers. If a parameter is nil, no filter will be used for that date part.
func UseSpecificGeolite2DateForTesting(yearRegex, monthRegex, dayRegex *string) {
	yearStr := `\d{4}`
	monthStr := `\d{2}`
	dayStr := monthStr

	if yearRegex != nil {
		yearStr = *yearRegex
	}
	if monthRegex != nil {
		monthStr = *monthRegex
	}
	if dayRegex != nil {
		dayStr = *dayRegex
	}

	geoLite2Regex = regexp.MustCompile(fmt.Sprintf(`Maxmind/%s/%s/%s/%s%s%sT\d{6}Z-GeoLite2-City-CSV\.zip`, yearStr, monthStr, dayStr, yearStr, monthStr, dayStr))
	geoLegacyRegex = regexp.MustCompile(fmt.Sprintf(`Maxmind/%s/%s/%s/%s%s%sT.*-GeoLiteCity.dat.*`, yearStr, monthStr, dayStr, yearStr, monthStr, dayStr))
	geoLegacyv6Regex = regexp.MustCompile(fmt.Sprintf(`Maxmind/%s/%s/%s/%s%s%sT.*-GeoLiteCityv6.dat.*`, yearStr, monthStr, dayStr, yearStr, monthStr, dayStr))
	_, file, line, _ := runtime.Caller(1)
	log.Printf("Date filter is set to %s%s%s by %s:%d", yearStr, monthStr, dayStr, file, line)
}

// UseOnlyMarchForTest hacks the regular expressions to reduce the number of datasets for testing.
func UseOnlyMarchForTest() {
	if flag.Lookup("test.v") == nil {
		log.Println("This should only be called in unit tests.")
		return
	}
	geoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/03/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)
	geoLegacyRegex = regexp.MustCompile(`Maxmind/\d{4}/03/\d{2}/\d{8}T.*-GeoLiteCity.dat.*`)
	geoLegacyv6Regex = regexp.MustCompile(`Maxmind/\d{4}/03/\d{2}/\d{8}T.*-GeoLiteCityv6.dat.*`)
}

/*****************************************************************************
*                          LoadAll... functions                              *
*****************************************************************************/

// Returns the normal iterator for objects in the appropriate GCS bucket.
func bucketIterator(withPrefix string) (*storage.ObjectIterator, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	prospectiveFiles := client.Bucket(api.MaxmindBucketName).Objects(ctx, &storage.Query{Prefix: withPrefix})
	return prospectiveFiles, nil
}

type Filename string

// loadSemaphore is used to control number of concurrent dataset loads.
// Spaces in the buffer act as tokens.  Loader adds a token to the
// channel on entry, and removes the token when it completes.
// It is shared across all loaders (that use loadAll), so it will limit
// concurrency across all datatypes.
var loadSemaphore = make(chan struct{}, 20)

// loadAll loads all datasets from the source that match the filter.
func loadAll(
	cache map[Filename]api.Annotator,
	filter func(file *storage.ObjectAttrs) error,
	loader func(*storage.ObjectAttrs) (api.Annotator, error),
	gcsPrefix string) (map[Filename]api.Annotator, error) {
	if loader == nil {
		return nil, ErrNoLoader
	}
	source, err := bucketIterator(gcsPrefix)
	if err != nil {
		return nil, err
	}

	// TODO - maybe use a channel and single threaded append instead.
	result := make(map[Filename]api.Annotator, len(cache)+2)
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
		filename := Filename(file.Name)

		// If the dataset is already loaded, skip to next one.
		ann, ok := cache[filename]
		if ok {
			result[filename] = ann
			continue
		}

		// If it has not yet been loaded, then load it now.
		_, _, callerLine, _ := runtime.Caller(1)
		log.Println("Loading", filename, "from line", callerLine)
		wg.Add(1)
		// wait for a semaphore to limit concurrency
		loadSemaphore <- struct{}{}
		go func(file *storage.ObjectAttrs) {
			defer func() { <-loadSemaphore }() // release semaphore on func exit.
			defer wg.Done()
			ann, err := loader(file)
			if err != nil {
				log.Println("Retrying", filename, "after", err)
				ann, err = loader(file)
				if err != nil {
					log.Println("Failed trying to load", filename, "with", err)
					return
				}
			}
			resultLock.Lock()
			result[filename] = ann
			resultLock.Unlock()
			metrics.DatasetCount.Inc()
			log.Println("Loaded", filename)
		}(file)
	}
	wg.Wait()
	return result, nil
}

// filter is used to create filter functions for the loaders.
// The file date is checked against `before` and file name is matched against `r`
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

// cachingLoader implements api.CachingLoader for legacy and geolite2 geolocation.
type cachingLoader struct {
	lock       sync.Mutex
	gcsPrefix  string
	annotators map[Filename]api.Annotator
	filter     func(*storage.ObjectAttrs) error
	loader     func(*storage.ObjectAttrs) (api.Annotator, error)
}

// UpdateCache causes the loader to load any new annotators and add them to the cached list.
func (cl *cachingLoader) UpdateCache() error {
	newMap, err :=
		loadAll(cl.annotators,
			func(file *storage.ObjectAttrs) error {
				return cl.filter(file)
			},
			cl.loader,
			cl.gcsPrefix)
	if err != nil {
		return err
	}
	cl.lock.Lock()
	defer cl.lock.Unlock()

	cl.annotators = newMap
	return nil
}

// Fetch returns a copy of the current list of annotators.
// The returned slice of Annotators is NOT sorted.
func (cl *cachingLoader) Fetch() []api.Annotator {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	result := make([]api.Annotator, 0, len(cl.annotators))
	for _, v := range cl.annotators {
		result = append(result, v)
	}
	return result
}

// NewCachingLoader creates a CachingLoader with the provided filter and loader.
func newCachingLoader(
	filter func(*storage.ObjectAttrs) error,
	loader func(*storage.ObjectAttrs) (api.Annotator, error),
	gcsPrefix string) api.CachingLoader {
	return &cachingLoader{filter: filter, loader: loader, annotators: make(map[Filename]api.Annotator, 100), gcsPrefix: gcsPrefix}
}

// LegacyV4Loader returns a CachingLoader that loads all v4 legacy datasets.
// The loader is injected, to allow for efficient unit testing.
func LegacyV4Loader(
	loader func(*storage.ObjectAttrs) (api.Annotator, error)) api.CachingLoader {
	return newCachingLoader(
		func(file *storage.ObjectAttrs) error {
			// We archived but do not use legacy datasets after GeoLite2StartDate.
			return filter(file, geoLegacyRegex, geoLite2StartDate)
		},
		loader,
		maxmindPrefix)
}

// LegacyV6Loader returns a CachingLoader that loads all v6 legacy datasets.
// The loader is injected, to allow for efficient unit testing.
func LegacyV6Loader(
	loader func(*storage.ObjectAttrs) (api.Annotator, error)) api.CachingLoader {
	return newCachingLoader(
		func(file *storage.ObjectAttrs) error {
			// We archived but do not use legacy datasets after GeoLite2StartDate.
			return filter(file, geoLegacyv6Regex, geoLite2StartDate)
		},
		loader,
		maxmindPrefix)
}

// Geolite2Loader returns a CachingLoader that loads all geolite2 datasets.
// The loader is injected, to allow for efficient unit testing.
func Geolite2Loader(
	loader func(*storage.ObjectAttrs) (api.Annotator, error)) api.CachingLoader {
	return newCachingLoader(
		func(file *storage.ObjectAttrs) error {
			return filter(file, geoLite2Regex, time.Time{})
		},
		loader,
		maxmindPrefix)
}

func IsLegacy(date time.Time) bool {
	return date.Before(geoLite2StartDate)
}
