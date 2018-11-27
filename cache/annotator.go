// This package handle the interface between handler and dataset.
package cache

import (
	"context"
	"errors"
	"log"
	"os"
	"regexp"
	"time"

	"cloud.google.com/go/storage"

	"github.com/m-lab/annotation-service/common"
	"github.com/m-lab/annotation-service/dataset"
	"github.com/m-lab/annotation-service/metrics"
	"github.com/m-lab/annotation-service/parser"

	"google.golang.org/api/iterator"
)

const (
	// This is the date we have the first GeoLite2 dataset.
	// Any request earlier than this date using legacy binary datasets
	// later than this date using GeoLite2 datasets
	GeoLite2CutOffDate = "August 15, 2017"

	// Folder containing the maxmind files
	MaxmindPrefix = "Maxmind/"
)

var (
	// This is a struct containing the latest data for the annotator to search
	// and reply with. The size of data map inside is 1.
	CurrentGeoDataset dataset.CurrentDatasetInMemory

	// The GeoLite2 datasets (except the current one) that are already in memory.
	Geolite2Dataset dataset.Geolite2DatasetInMemory

	// The legacy datasets that are already in memory.
	LegacyDataset dataset.LegacyDatasetInMemory

	// This is the bucket containing maxmind files.
	BucketName = "downloader-" + os.Getenv("GCLOUD_PROJECT")
)

// DatasetNames are list of datasets sorted in lexographical order in downloader bucket.
var DatasetNames []string

// The date of lastest available dataset.
var LatestDatasetDate time.Time

// This is the regex used to filter for which files we want to consider acceptable for using with legacy dataset
var GeoLegacyRegex = regexp.MustCompile(`.*-GeoLiteCity.dat.*`)
var GeoLegacyv6Regex = regexp.MustCompile(`.*-GeoLiteCityv6.dat.*`)

// This is the regex used to filter for which files we want to consider acceptable for using with Geolite2
var GeoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)

// GetMetadataForSingleIP takes a pointer to a common.RequestData
// struct and will use it to fetch the appropriate associated
// metadata, returning a pointer. It is gaurenteed to return a non-nil
// pointer, even if it cannot find the appropriate metadata.
func Annotate(request *common.RequestData) (*common.GeoData, error) {
	metrics.Metrics_totalLookups.Inc()

	if request.Timestamp.After(LatestDatasetDate) {
		return CurrentGeoDataset.GetGeoLocationForSingleIP(request, "")
	}

	isIP4 := true
	if request.IPFormat == 6 {
		isIP4 = false
	}

	filename, err := SelectArchivedDataset(request.Timestamp, dataset.BucketName, isIP4)

	//log.Println(filename)
	if err != nil {
		return nil, errors.New("Cannot get historical dataset")
	}
	if GeoLite2Regex.MatchString(filename) {
		return Geolite2Dataset.GetGeoLocationForSingleIP(request, filename)
	} else {
		return LegacyDataset.GetGeoLocationForSingleIP(request, filename)
	}
}

// SelectArchivedDataset returns the archived GelLite dataset filename given a date.
// For any input date earlier than 2013/08/28, we will return 2013/08/28 dataset.
// For any input date later than latest available dataset, we will return the latest dataset
// Otherwise, we return the last dataset before the input date.
func SelectArchivedDataset(requestDate time.Time, bucketName string, isIP4 bool) (string, error) {
	earliestArchiveDate, _ := time.Parse("January 2, 2006", "August 28, 2013")
	if requestDate.Before(earliestArchiveDate) {
		return "Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz", nil
	}
	CutOffDate, _ := time.Parse("January 2, 2006", GeoLite2CutOffDate)
	lastFilename := ""
	for _, fileName := range DatasetNames {
		if requestDate.Before(CutOffDate) && ((isIP4 && GeoLegacyRegex.MatchString(fileName)) || (!isIP4 && GeoLegacyv6Regex.MatchString(fileName))) {
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

// ExtractDateFromFilename return the date for a filename like
// gs://downloader-mlab-oti/Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz
func ExtractDateFromFilename(filename string) (time.Time, error) {
	re := regexp.MustCompile(`[0-9]{8}T`)
	filedate := re.FindAllString(filename, -1)
	if len(filedate) != 1 {
		return time.Time{}, errors.New("cannot extract date from input filename")
	}
	return time.Parse(time.RFC3339, filedate[0][0:4]+"-"+filedate[0][4:6]+"-"+filedate[0][6:8]+"T00:00:00Z")
}

// Init extracts the filenames from downloader bucket
// It also searches the latest Geolite2 files available in GCS
// and will use them to create a new GeoDataset which
// it will place into the global scope as the latest version.
// If it encounters an error, it will halt the program.
// It will also set LatestDatasetDate as the date of lastest dataset.
func Init() error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	prospectiveFiles := client.Bucket(BucketName).Objects(ctx, &storage.Query{Prefix: MaxmindPrefix})
	filename := ""
	for file, err := prospectiveFiles.Next(); err != iterator.Done; file, err = prospectiveFiles.Next() {
		if err != nil {
			return err
		}
		DatasetNames = append(DatasetNames, file.Name)
		if file.Name > filename && GeoLite2Regex.MatchString(file.Name) {
			filename = file.Name
		}
	}

	if err != nil {
		log.Println(err)
	}

	CurrentGeoDataset.Init()
	Geolite2Dataset.Init()
	LegacyDataset.Init()

	// Now set the lastest dataset
	date, err := ExtractDateFromFilename(filename)
	if err != nil {
		log.Println(err)
		return err
	}
	LatestDatasetDate = date
	CurrentGeoDataset.AddDataset(filename)
	return nil
}

// SetLatestDataset sets the latest dataset directly.
func SetLatestDataset(p *parser.GeoDataset) {
	CurrentGeoDataset.SetDataset(p)
}
