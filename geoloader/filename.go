package geoloader

import (
	"context"
	"log"
	"regexp"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/m-lab/annotation-service/api"
)

// GeoLite2StartDate is the date we have the first GeoLite2 dataset.
// Any request earlier than this date using legacy binary datasets
// later than this date using GeoLite2 datasets
var GeoLite2StartDate = time.Unix(1502755200, 0) //"August 15, 2017"

// EarliestArchiveDate is the date of the earliest archived dataset.
var EarliestArchiveDate = time.Unix(1377648000, 0) // "August 28, 2013")

var AllDatasets = Directory{sync.RWMutex{}, make(map[string]string, 100), make([]string, 0, 100), time.Time{}}

// DatasetFilenames are list of datasets sorted in lexographical order in downloader bucket.
type Directory struct {
	mutex     sync.RWMutex
	filenames map[string]string
	dates     []string
	// The date of lastest available dataset.
	latestDate time.Time
}

// Latest returns the date of the latest dataset.
// Caller must NOT hold lock.
func (dir *Directory) Latest() time.Time {
	dir.mutex.RLock()
	defer dir.mutex.RUnlock()
	return dir.latestDate
}

// Caller MUST hold lock.
func (dir *Directory) add(date time.Time, fn string) {
	if len(dir.filenames) == 0 {
		dir.latestDate = date
	} else if date.After(dir.latestDate) {
		dir.latestDate = date
	}
	dateString := date.Format("20060102")
	dir.filenames[dateString] = fn
}

// Caller MUST hold lock.
func (dir *Directory) sort() {
	dir.dates = make([]string, 0, len(dir.filenames))
	for k := range dir.filenames {
		dir.dates = append(dir.dates, k)
	}
	sort.Strings(dir.dates)
}

// LastBefore returns the filename associated with the provided date.
// Caller must NOT hold lock.
func (dir *Directory) LastBefore(date time.Time) string {
	dir.mutex.RLock()
	defer dir.mutex.RUnlock()

	if len(dir.dates) == 0 {
		return ""
	}

	dateString := date.Format("20060102")
	// TODO use sort.SearchString()
	index := sort.SearchStrings(dir.dates, dateString)
	if index == 0 {
		return dir.filenames[dir.dates[index]]
	} else {
		return dir.filenames[dir.dates[index-1]]
	}
}

// This is the regex used to filter for which files we want to consider acceptable for using with Geolite2
var GeoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)

// This is the regex used to filter for which files we want to consider acceptable for using with legacy dataset
var GeoLegacyRegex = regexp.MustCompile(`.*-GeoLiteCity.dat.*`)
var GeoLegacyv6Regex = regexp.MustCompile(`.*-GeoLiteCityv6.dat.*`)

// UpdateArchivedFilenames extracts the dataset filenames from downloader bucket
// It also searches the latest Geolite2 files available in GCS.
// It will also set LatestDatasetDate as the date of lastest dataset.
// This job was run at the beginning of deployment and daily cron job.
func (dir *Directory) UpdateArchivedFilenames() error {
	dir.mutex.Lock()
	defer dir.mutex.Unlock()
	dir.filenames = make(map[string]string, len(dir.filenames))

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	prospectiveFiles := client.Bucket(api.MaxmindBucketName).Objects(ctx, &storage.Query{Prefix: api.MaxmindPrefix})
	lastFilename := ""
	for file, err := prospectiveFiles.Next(); err != iterator.Done; file, err = prospectiveFiles.Next() {
		if err != nil {
			return err
		}
		if !GeoLite2Regex.MatchString(file.Name) && !GeoLegacyRegex.MatchString(file.Name) && !GeoLegacyv6Regex.MatchString(file.Name) {
			continue
		}
		// We archived but do not use legacy datasets after GeoLite2StartDate.
		fileDate, err := api.ExtractDateFromFilename(file.Name)
		if err != nil {
			continue
		}
		if !fileDate.Before(GeoLite2StartDate) && !GeoLite2Regex.MatchString(file.Name) {
			continue
		}

		dir.add(fileDate, file.Name)
		// Files are ordered lexicographically, and the naming convention means that
		// the last file in the list will be the most recent
		if file.Name > lastFilename && GeoLite2Regex.MatchString(file.Name) {
			lastFilename = file.Name
		}
	}
	if err != nil {
		log.Println(err)
	}

	dir.sort()

	return nil
}
