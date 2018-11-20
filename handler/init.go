package handler

import (
	"context"
	"errors"
	"log"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/handler/dataset"
	"google.golang.org/api/iterator"
)

// The date of lastest available dataset.
var LatestDatasetDate time.Time

// DatasetNames are list of datasets sorted in lexographical order in downloader bucket.
var DatasetNames []string

// determineFilenameOfLatestGeolite2File will get a list of filenames
// from GCS and search through them, eventually returing either the
// latest filename or an error.
func determineFilenameOfLatestGeolite2File() (string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", err
	}
	prospectiveFiles := client.Bucket(dataset.BucketName).Objects(ctx, &storage.Query{Prefix: dataset.MaxmindPrefix})
	filename := ""
	for file, err := prospectiveFiles.Next(); err != iterator.Done; file, err = prospectiveFiles.Next() {
		if err != nil {
			return "", err
		}
		if file.Name > filename && GeoLite2Regex.MatchString(file.Name) {
			filename = file.Name
		}

	}
	return filename, nil
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

// PopulateLatestData will search to the latest Geolite2 files
// available in GCS and will use them to create a new GeoDataset which
// it will place into the global scope as the latest version. It will
// do so safely with use of the currentDataMutex RW mutex. It it
// encounters an error, it will halt the program.
// It will also set LatestDatasetDate as the date of lastest dataset.
func PopulateLatestData() {
	filename, err := determineFilenameOfLatestGeolite2File()
	if err != nil {
		log.Fatal(err)
	}
	LatestDatasetDate, err = ExtractDateFromFilename(filename)

	CurrentGeoDataset.AddDataset(filename)
}

// UpdateFilenamelist extracts the filenames from downloader bucket.
// DatasetNames are sorted in lexographical order.
func UpdateFilenamelist(bucketName string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	prospectiveFiles := client.Bucket(bucketName).Objects(ctx, &storage.Query{Prefix: dataset.MaxmindPrefix})

	for file, err := prospectiveFiles.Next(); err != iterator.Done; file, err = prospectiveFiles.Next() {
		if err != nil {
			return err
		}
		DatasetNames = append(DatasetNames, file.Name)
	}

	if err != nil {
		log.Println(err)
	}
	CurrentGeoDataset.Init()
	Geolite2Dataset.Init()
	LegacyDataset.Init()

	return nil
}
