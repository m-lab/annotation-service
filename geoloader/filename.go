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

// datasetDirV4 and datasetDirV6 stores info on all the available datasets.  It is initially empty, just to
// provide the LatestDate() function.
// The current directory is regarded as immutable, but the pointer is dynamically updated, so accesses
// should only be done through getDirectory() and setDirectory().
var datasetDirV4 = &directory{}
var datasetDirV6 = &directory{}
var datasetDirLock sync.RWMutex // lock to be held when accessing or updating datasetDir pointer.

var DatasetFilenames []string

func getDirectoryV4() *directory {
	datasetDirLock.RLock()
	defer datasetDirLock.RUnlock()
	return datasetDirV4
}

func getDirectoryV6() *directory {
	datasetDirLock.RLock()
	defer datasetDirLock.RUnlock()
	return datasetDirV6
}

func setDirectory(dirv4 *directory, dirv6 *directory) {
	datasetDirLock.Lock()
	defer datasetDirLock.Unlock()
	datasetDirV4 = dirv4
	datasetDirV6 = dirv6
}

type dateEntry struct {
	date     time.Time
	filename string
}

// directory maintains a list of datasets.
type directory struct {
	entries map[string]*dateEntry // Map to filename associated with date.
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
		entry = &dateEntry{filename: fn, date: date}
		dir.entries[dateString] = entry
	}
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
		return ""
	}

	dateString := date.Format("20060102")
	index := sort.SearchStrings(dir.dates, dateString)
	if index == 0 {
		return dir.entries[dir.dates[index]].filename
	}
	return dir.entries[dir.dates[index-1]].filename
}

// TODO: These regex are duplicated in geolite2 and legacy packages.
// This is the regex used to filter for which files we want to consider acceptable for using with Geolite2
var GeoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)

// This is the regex used to filter for which files we want to consider acceptable for using with legacy dataset
var GeoLegacyRegex = regexp.MustCompile(`.*-GeoLiteCity.dat.*`)
var GeoLegacyv6Regex = regexp.MustCompile(`.*-GeoLiteCityv6.dat.*`)

// UpdateArchivedFilenames extracts the dataset filenames from downloader bucket
// This job is run at the beginning of deployment and daily cron job.
func UpdateArchivedFilenames() error {
	sizev4 := len(getDirectoryV4().dates) + 2
	sizev6 := len(getDirectoryV6().dates) + 2
	dirV4 := directory{entries: make(map[string]*dateEntry, sizev4), dates: make([]string, 0, sizev4)}
	dirV6 := directory{entries: make(map[string]*dateEntry, sizev6), dates: make([]string, 0, sizev6)}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
        DatasetFilenames = []
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
		IPtype := 0
		if fileDate.Before(GeoLite2StartDate) {
			// Check whether thhis legacy dataset is IPv4 or IPv6
			if GeoLegacyRegex.MatchString(file.Name) {
				IPtype = 4
			} else if GeoLegacyv6Regex.MatchString(file.Name) {
				IPtype = 6
			} else {
				continue
			}
		}

		if !fileDate.Before(GeoLite2StartDate) && !GeoLite2Regex.MatchString(file.Name) {
			continue
		}

		// Build 2 dirs here. One for IPv4 and one for IPv6
		if IPtype == 4 {
			dirV4.Insert(fileDate, file.Name)
		} else if IPtype == 6 {
			dirV6.Insert(fileDate, file.Name)
		} else {
			dirV4.Insert(fileDate, file.Name)
			dirV6.Insert(fileDate, file.Name)
		}
		DatasetFilenames = append(DatasetFilenames, file.Name)
	}
	if err != nil {
		log.Println(err)
	}

	setDirectory(&dirV4, &dirV6)
	return nil
}

// Latest returns the date of the latest dataset.
// May return time.Time{} if no dates have been loaded.
func LatestDatasetDate() time.Time {
	dd := getDirectoryV4()
	return dd.latestDate()
}

// BestAnnotatorFilename return legacy IPv4 or IPv6 or Geolite2 filename based on request date and IP type
func BestAnnotatorFilename(request *api.RequestData) string {
	if request.IPFormat == 4 {
		dd := getDirectoryV4()
		return dd.LastFilenameEarlierThan(request.Timestamp)
	} else if request.IPFormat == 6 {
		dd := getDirectoryV6()
		return dd.LastFilenameEarlierThan(request.Timestamp)
	} else {
		return ""
	}
}
