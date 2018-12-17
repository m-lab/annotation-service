package geoloader

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/m-lab/annotation-service/api"
)

// DatasetInfo stores all info related to a dataset file.
// Note that in future, Annotator for legacy dataset will wrap two files.
type DatasetInfo struct {
	Path     string
	Exp      string
	DateDir  string
	DateTime string
	FnRoot   string // The root of the filename, (before any '.')
	Latest   string
	FnExt    string
	Version  int  // 1 = legacy, 2 = GeoLite2
	IsV6     bool // True if this is a legacy V6 dataset
}

var (
	root       = `^(.*)/`               // 1
	dir        = `(\d{4}/\d{2}/\d{2})/` // 2
	dateTime   = `(\d{8})T(.*)Z-`       // 3 4
	fn         = `(GeoLite.*?)`         // 5
	latest     = `(-latest)?`           // 6
	v6         = `([vV]6)?\.`           // 7
	fext       = `(.*)$`                // 8
	filenameRE = regexp.MustCompile(root + dir + dateTime + fn + latest + v6 + fext)
)

// ParseDataset parses a dataset filename and returns a DatasetInfo
func ParseDataset(fn string) DatasetInfo {
	parts := filenameRE.FindStringSubmatch(fn)
	switch len(parts) {
	case 0:
		return DatasetInfo{}
	case 1:
		return DatasetInfo{Path: parts[0]}
	default:
		df := DatasetInfo{Path: parts[0],
			Exp:      parts[1],
			DateDir:  parts[2],
			DateTime: fmt.Sprintf("%sT%sZ", parts[3], parts[4]),
			Latest:   parts[6],
			FnRoot:   parts[5], FnExt: parts[8]}
		df.IsV6 = strings.ToLower(parts[7]) == "v6"
		if df.FnRoot == "GeoLiteCity" {
			df.Version = 1
		} else {
			df.Version = 2
		}
		return df
	}
}

// GeoLite2StartDate is the date we have the first GeoLite2 dataset.
// Any request earlier than this date using legacy binary datasets
// later than this date using GeoLite2 datasets
var GeoLite2StartDate = time.Unix(1502755200, 0) //"August 15, 2017"

// EarliestArchiveDate is the date of the earliest archived dataset.
var EarliestArchiveDate = time.Unix(1377648000, 0) // "August 28, 2013")

// DatasetFilenames are list of datasets sorted in lexographical order in downloader bucket.
var DatasetFilenames []string

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
	DatasetFilenames = make([]string, 50)
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
		// TODO use this instead of the individual regular expressions
		df := ParseDataset(file.Name)
		log.Printf("%s\n", df.Path)

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
		DatasetFilenames = append(DatasetFilenames, file.Name)
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
