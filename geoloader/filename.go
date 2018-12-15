package geoloader

import (
	"context"
	"log"
	"regexp"
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

// DatasetFilenames are list of datasets sorted in lexographical order in downloader bucket.
// TODO make this an object
var DatasetFilenames map[string]string

// The date of lastest available dataset.
var LatestDatasetDate time.Time

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
	DatasetFilenames = make(map[string]string, 50)
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
		dateString := fileDate.Format("20060102")
		DatasetFilenames[dateString] = file.Name
		// Files are ordered lexicographically, and the naming convention means that
		// the last file in the list will be the most recent
		if file.Name > lastFilename && GeoLite2Regex.MatchString(file.Name) {
			lastFilename = file.Name
		}
	}
	if err != nil {
		log.Println(err)
	}
	// Now set the lastest dataset
	date, err := api.ExtractDateFromFilename(lastFilename)
	if err != nil {
		log.Println(err)
		return err
	}
	LatestDatasetDate = date
	return nil
}
