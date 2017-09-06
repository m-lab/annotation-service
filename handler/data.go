package handler

import (
	"context"
	"errors"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/parser"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

// This is the regex used to filter for which files we want to consider acceptable for using with Geolite2
var GeoLite2Regex = regexp.MustCompile(`Maxmind/(\d{4}/\d{2}/\d{2})/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)

// This is the regex used to filter for which files we want to consider acceptable for using with Geolite1
var GeoLite1Regex = regexp.MustCompile(`Maxmind/(\d{4}/\d{2}/\d{2})/\d{8}T\d{6}Z-GeoLiteCity-latest\.zip`)

var BucketName = "downloader-" + os.Getenv("GCLOUD_PROJECT") // This is the bucket containing maxmind files

const (
	MaxmindPrefix = "Maxmind/" // Folder containing the maxmind files

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
	currentGeoDataset = data
	currentDataMutex.Unlock()
}

// DetermineFilenameOfLatestGeolite2File will get a list of filenames
// from GCS and search through them, eventually returing either the
// latest filename or an error.
func DetermineFilenameOfLatestGeolite2File() (string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", err
	}
	prospectiveFiles := client.Bucket(BucketName).Objects(ctx, &storage.Query{Prefix: MaxmindPrefix})
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

// LoadLatestGeolite2File will check GCS for the latest file, download
// it, process it, and load it into memory so that it can be easily
// searched, then it will return a pointer to that GeoDataset or an error.
func LoadLatestGeolite2File() (*parser.GeoDataset, error) {
	filename, err := DetermineFilenameOfLatestGeolite2File()
	if err != nil {
		return nil, err
	}
	zip, err := loader.CreateZipReader(context.Background(), BucketName, filename)
	if err != nil {
		return nil, err
	}
	return parser.LoadGeoLite2(zip)
}

// LoadGeoDataset takes a timestamp and downloads and sets up the
// appropriate GeoDataset before returning the pointer to it, or an
// error if it encounters any
func LoadGeoDataset(timestamp time.Time) (*parser.GeoDataset, error) {
	filename, geoVersion, err := FindGeofileForTime(timestamp)
	if err != nil {
		return nil, err
	}
	zip, err := loader.CreateZipReader(context.Background(), BucketName, filename)
	if err != nil {
		return nil, err
	}
	if geoVersion == 2 {
		return parser.LoadGeoLite2(zip)
	} else if geoVersion == 1 {
		return parser.LoadGeoLite1(zip)
	}
	return nil, errors.New("Unknown Geolite version!")
}

// FindGeofileForTime takes a timestamp and returns a string
// specifying the file to load to create the appropriate GeoDataset
// from that timestamp, as well as an int specifying the version of
// the geolite database to load. If it encounters an error, it will
// return that instead.
func FindGeofileForTime(timestamp time.Time) (string, int, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", 0, err
	}
	prospectiveFiles := client.Bucket(BucketName).Objects(ctx, &storage.Query{Prefix: MaxmindPrefix})
	timeStr := timestamp.Format("2006/01/02")
	// We need to search for glite1 and glite2 separately, since we don't know which we'll have
	glite2Candidate := ""
	glite1Candidate := ""
	for file, err := prospectiveFiles.Next(); err != iterator.Done; file, err = prospectiveFiles.Next() {
		if err != nil {
			return "", 0, err
		}
		glite2Match := GeoLite2Regex.FindStringSubmatch(file.Name)
		// Select the file if it is newer that the one we have, but not newer than the timestamp
		if glite2Candidate < file.Name && glite2Match != nil && glite2Match[1] < timeStr {
			glite2Candidate = file.Name
		}

		glite1Match := GeoLite1Regex.FindStringSubmatch(file.Name)
		if glite1Candidate < file.Name && glite1Match != nil && glite1Match[1] < timeStr {
			glite1Candidate = file.Name
		}

	}
	// Attempt to get the timestamp from the glite2 file, if we can't, then use glite1
	glite2Match := GeoLite2Regex.FindStringSubmatch(glite2Candidate)
	if glite2Match == nil {
		return glite1Candidate, 1, nil
	}
	glite2Timestamp, err := time.Parse("2006/01/02", glite2Match[1])
	if err != nil {
		return glite1Candidate, 1, nil
	}

	// If the glite2 file is off by 40 days or more, fallback to glite1, otherwise use glite2
	if glite2Timestamp.Sub(timestamp) >= 24*time.Hour*40 {
		return glite1Candidate, 1, nil
	}
	return glite2Candidate, 2, nil
}

// ChooseGeoDataset will attempt to select a GeoDataset from the
// currently loaded sets, given a timestamp. If it finds a mattching
// one, it will return a pointer to it. If it doesn't, then it will
// return an error.
func ChooseGeoDataset(timestamp time.Time) (*parser.GeoDataset, error) {
	// TODO: Make an actual implementation
	return nil, errors.New("Function not yet implemented!")
}
