package handler

import (
	"context"
	"errors"
	"log"
	"time"

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
	CurrentGeoDataset = data
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

func LoadGeoLite2Dataset(requestDate time.Time, bucketName string) (*parser.GeoDataset, error) {
	CutOffDate, _ := time.Parse("January 2, 2006", GeoLite2CutOffDate)
	if !requestDate.Before(CutOffDate) {
		filename, err := SelectGeoLegacyFile(requestDate, bucketName)
		if err != nil {
			return nil, err
		}
		// load GeoLite2 dataset
		zip, err := loader.CreateZipReader(context.Background(), bucketName, filename)
		if err != nil {
			return nil, err
		}
		return parser.LoadGeoLite2(zip)
	}
	return nil, errors.New("should call LoadLegacyGeoliteDataset with input date")
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

// ConvertIPNodeToGeoData takes a parser.IPNode, plus a list of
// locationNodes. It will then use that data to fill in a GeoData
// struct and return its pointer.
func ConvertIPNodeToGeoData(ipNode parser.IPNode, locationNodes []parser.LocationNode) *common.GeoData {
	locNode := parser.LocationNode{}
	if ipNode.LocationIndex >= 0 {
		locNode = locationNodes[ipNode.LocationIndex]
	}
	return &common.GeoData{
		Geo: &common.GeolocationIP{
			Continent_code: locNode.ContinentCode,
			Country_code:   locNode.CountryCode,
			Country_code3:  "", // missing from geoLite2 ?
			Country_name:   locNode.CountryName,
			Region:         locNode.RegionCode,
			Metro_code:     locNode.MetroCode,
			City:           locNode.CityName,
			Area_code:      0, // new geoLite2 does not have area code.
			Postal_code:    ipNode.PostalCode,
			Latitude:       ipNode.Latitude,
			Longitude:      ipNode.Longitude,
		},
		ASN: &common.IPASNData{},
	}

}
