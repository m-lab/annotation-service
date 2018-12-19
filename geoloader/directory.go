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
// TODO make this local
var GeoLite2StartDate = time.Unix(1502755200, 0) //"August 15, 2017"

// earliestArchiveDate is the date of the earliest archived dataset.
var earliestArchiveDate = time.Unix(1377648000, 0) // "August 28, 2013")

// datasetDir stores info on all the available datasets.  It is initially empty, just to
// provide the LatestDate() function.
// The current directory is static.  But the pointer is dynamically updated, so accesses
// should only be done through GetDirectory()
var datasetDir = &directory{}
var mutex sync.RWMutex

type dateEntry struct {
	date      time.Time
	filenames []string
}

// directory maintains a list of datasets.
type directory struct {
	entries map[string]*dateEntry
	dates   []string
	// The date of lastest available dataset.
	latestDate time.Time
}

func newDirectory(size int) directory {
	return directory{entries: make(map[string]*dateEntry, size), dates: make([]string, 0, size)}
}

// Insert inserts a new filename into the directory at the given date.
// NOTE: This does not detect or eliminate duplicates.
func (dir *directory) Insert(date time.Time, fn string) {
	if len(dir.entries) == 0 {
		dir.latestDate = date
	} else if date.After(dir.latestDate) {
		dir.latestDate = date
	}
	dateString := date.Format("20060102")
	entry, ok := dir.entries[dateString]
	if !ok {
		// Insert the new date into the date slice.
		index := sort.SearchStrings(dir.dates, dateString)
		dir.dates = append(dir.dates, "")
		copy(dir.dates[index+1:], dir.dates[index:])
		dir.dates[index] = dateString

		// Create new entry for the date.
		entry = &dateEntry{filenames: make([]string, 0, 3)}
		dir.entries[dateString] = entry
	}

	entry.filenames = append(entry.filenames, fn)
}

// LastBefore returns the filename associated with the provided date.
// Caller must NOT hold lock.
func (dir *directory) LastBefore(date time.Time) string {
	if len(dir.dates) == 0 {
		return ""
	}

	// Add one day, so that we use dataset for same date published
	// TODO perhaps we should use the previous dataset???
	date = date.Add(24 * time.Hour)

	dateString := date.Format("20060102")
	index := sort.SearchStrings(dir.dates, dateString)
	if index == 0 {
		return dir.entries[dir.dates[index]].filenames[0]
	}
	return dir.entries[dir.dates[index-1]].filenames[0]
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
func UpdateArchivedFilenames() error {
	dir := directory{entries: make(map[string]*dateEntry, 100), dates: make([]string, 0, 100)}

	dir.entries = make(map[string]*dateEntry, len(dir.entries)+2)

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

		dir.Insert(fileDate, file.Name)
		// Files are ordered lexicographically, and the naming convention means that
		// the last file in the list will be the most recent
		if file.Name > lastFilename && GeoLite2Regex.MatchString(file.Name) {
			lastFilename = file.Name
		}
	}
	if err != nil {
		log.Println(err)
	}

	mutex.Lock()
	datasetDir = &dir
	mutex.Unlock()

	return nil
}

func getDirectory() *directory {
	mutex.RLock()
	defer mutex.RUnlock()
	return datasetDir
}

// Latest returns the date of the latest dataset.
func Latest() time.Time {
	dd := getDirectory()
	return dd.latestDate
}

// LastBefore returns the dataset filename for annotating the requested date.
func LastBefore(date time.Time) string {
	dd := getDirectory()
	return dd.LastBefore(date)
}
