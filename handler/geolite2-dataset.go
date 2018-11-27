package handler

import (
	"context"
	"log"

	"github.com/m-lab/annotation-service/common"
	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/parser"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

// PopulateLatestData will search to the latest Geolite2 files
// available in GCS and will use them to create a new GeoDataset which
// it will place into the global scope as the latest version. It will
// do so safely with use of the currentDataMutex RW mutex. It it
// encounters an error, it will halt the program.
func PopulateLatestData() {
	data, err := LoadLatestGeolite2File()
	if err != nil {
		log.Fatal(err)
	}
	currentDataMutex.Lock()
	CurrentAnnotator = data
	currentDataMutex.Unlock()
}

// determineFilenameOfLatestGeolite2File will get a list of filenames
// from GCS and search through them, eventually returing either the
// latest filename or an error.
func determineFilenameOfLatestGeolite2File() (string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", err
	}
	prospectiveFiles := client.Bucket(common.MaxmindBucketName).Objects(ctx, &storage.Query{Prefix: common.MaxmindPrefix})
	filename := ""
	for file, err := prospectiveFiles.Next(); err != iterator.Done; file, err = prospectiveFiles.Next() {
		if err != nil {
			return "", err
		}
		if file.Name > filename && common.GeoLite2Regex.MatchString(file.Name) {
			filename = file.Name
		}

	}
	return filename, nil
}

// LoadGeoLite2Dataset load the Geolite2 dataset with filename from bucket.
func LoadGeoLite2Dataset(filename string, bucketname string) (*parser.GeoDataset, error) {
	zip, err := loader.CreateZipReader(context.Background(), bucketname, filename)
	if err != nil {
		return nil, err
	}
	return parser.LoadGeoLite2(zip)
}

// LoadLatestGeolite2File will check GCS for the latest file, download
// it, process it, and load it into memory so that it can be easily
// searched, then it will return a pointer to that GeoDataset or an error.
func LoadLatestGeolite2File() (*parser.GeoDataset, error) {
	filename, err := determineFilenameOfLatestGeolite2File()
	if err != nil {
		return nil, err
	}
	return LoadGeoLite2Dataset(filename, common.MaxmindBucketName)
}
