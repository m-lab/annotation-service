package geoloader

import (
	"context"
	"flag"
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
// The current directory is regarded as immutable, but the pointer is dynamically updated, so accesses
// should only be done through getDirectory() and setDirectory().
var datasetDir *directory
var datasetDirLock sync.RWMutex // lock to be held when accessing or updating datasetDir pointer.

func getDirectory() *directory {
	datasetDirLock.RLock()
	defer datasetDirLock.RUnlock()
	return datasetDir
}

func setDirectory(dir *directory) {
	datasetDirLock.Lock()
	defer datasetDirLock.Unlock()
	datasetDir = dir
}

func init() {
	dir := newDirectory(10)
	// Hack
	if flag.Lookup("test.v") != nil {
		date, _ := time.Parse("20060102", "20130828")
		dir.Insert(date, "Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz")
	}
	setDirectory(&dir)
}

type dateEntry struct {
	date      time.Time
	filenames []string
}

// directory maintains a list of datasets.
type directory struct {
	entries map[string]*dateEntry // Map to filenames associated with date.
	dates   []string              // Date strings associated with files.
}

func newDirectory(size int) directory {
	return directory{entries: make(map[string]*dateEntry, size), dates: make([]string, 0, size)}
}

// Insert inserts a new filename into the directory at the given date.
// NOTE: This does not detect or eliminate duplicates.
// TODO - make this local.
func (dir *directory) Insert(date time.Time, fn string) {
	dateString := date.Format("20060102")
	entry, ok := dir.entries[dateString]
	if !ok {
		// Insert the new date into the date slice.
		index := sort.SearchStrings(dir.dates, dateString)
		dir.dates = append(dir.dates, "")
		copy(dir.dates[index+1:], dir.dates[index:])
		dir.dates[index] = dateString

		// Create new entry for the date.
		entry = &dateEntry{filenames: make([]string, 0, 2), date: date}
		dir.entries[dateString] = entry
	}

	log.Println("Adding", dateString, fn)
	entry.filenames = append(entry.filenames, fn)
}

func (dir *directory) latestDate() time.Time {
	if len(dir.dates) < 1 {
		return time.Time{}
	}
	d := dir.dates[len(dir.dates)-1]
	return dir.entries[d].date
}

// LastFilenameEarlierThan returns the filename associated with the provided date.
// Except for dates prior to 2013, it will return the latest filename with date prior
// to the provided date.
// Returns empty string if the directory is empty.
func (dir *directory) LastFilenameEarlierThan(date time.Time) string {
	if len(dir.dates) == 0 {
		log.Println("ERROR - no filenames")
		return ""
	}

	dateString := date.Format("20060102")
	index := sort.SearchStrings(dir.dates, dateString)
	if index == 0 {
		return dir.entries[dir.dates[index]].filenames[0]
	}
	return dir.entries[dir.dates[index-1]].filenames[0]
}

// TODO: These regex are duplicated in geolite2 and legacy packages.
// This is the regex used to filter for which files we want to consider acceptable for using with Geolite2
var GeoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)

// This is the regex used to filter for which files we want to consider acceptable for using with legacy dataset
var GeoLegacyRegex = regexp.MustCompile(`.*-GeoLiteCity.dat.*`)
var GeoLegacyv6Regex = regexp.MustCompile(`.*-GeoLiteCityv6.dat.*`)

// UpdateArchivedFilenames updates the list of dataset filenames from GCS.
// This job is run at the beginning of deployment and daily cron job.
func UpdateArchivedFilenames() error {
	old := getDirectory()
	size := len(old.dates) + 2
	dir := directory{entries: make(map[string]*dateEntry, size), dates: make([]string, 0, size)}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	prospectiveFiles := client.Bucket(api.MaxmindBucketName).Objects(ctx, &storage.Query{Prefix: api.MaxmindPrefix})
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
	}
	if err != nil {
		log.Println(err)
	}

	setDirectory(&dir)

	return nil
}

// BestAnnotatorName returns the dataset filename for annotating the requested date.
func BestAnnotatorName(date time.Time) string {
	dd := getDirectory()
	return dd.LastFilenameEarlierThan(date)
}
