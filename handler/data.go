package handler

import (
	"context"
	"log"
	"os"
	"regexp"

	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/parser"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

// The GeoDataset struct bundles all the data needed to search and
// find data into one common structure
type GeoDataset struct {
	IP4Nodes      []parser.IPNode       // The IPNode list containing IP4Nodes
	IP6Nodes      []parser.IPNode       // The IPNode list containing IP6Nodes
	LocationNodes []parser.LocationNode // The location nodes corresponding to the IPNodes
}

// This is the regex used to filter for which files we want to consider acceptable for using with Geolite2
var GeoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)

var BucketName = "downloader-" + os.Getenv("GCLOUD_PROJECT") // This is the bucket containing maxmind files

const (
	MaxmindPrefix             = "Maxmind/"                       // Folder containing the maxmind files
	GeoLite2BlocksFilenameIP4 = "GeoLite2-City-Blocks-IPv4.csv"  // Filename of ipv4 blocks file
	GeoLite2BlocksFilenameIP6 = "GeoLite2-City-Blocks-IPv6.csv"  // Filename of ipv6 blocks file
	GeoLite2LocationsFilename = "GeoLite2-City-Locations-en.csv" // Filename of locations file
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
// searched, then it will return a pointer to that file or an error.
func LoadLatestGeolite2File() (*GeoDataset, error) {
	filename, err := DetermineFilenameOfLatestGeolite2File()
	if err != nil {
		return nil, err
	}
	zip, err := loader.CreateZipReader(context.Background(), BucketName, filename)
	if err != nil {
		return nil, err
	}
	locations, err := loader.FindFile(GeoLite2LocationsFilename, zip)
	if err != nil {
		return nil, err
	}
	// geoidMap is just a temporary map that will be discarded once the blocks are parsed
	locationNodes, geoidMap, err := parser.CreateLocationList(locations)
	if err != nil {
		return nil, err
	}
	blocks4, err := loader.FindFile(GeoLite2BlocksFilenameIP4, zip)
	if err != nil {
		return nil, err
	}
	ipNodes4, err := parser.CreateIPList(blocks4, geoidMap, "GeoLite2-City-Blocks")
	if err != nil {
		return nil, err
	}
	blocks6, err := loader.FindFile(GeoLite2BlocksFilenameIP6, zip)
	if err != nil {
		return nil, err
	}
	ipNodes6, err := parser.CreateIPList(blocks6, geoidMap, "GeoLite2-City-Blocks")
	if err != nil {
		return nil, err
	}
	return &GeoDataset{IP4Nodes: ipNodes4, IP6Nodes: ipNodes6, LocationNodes: locationNodes}, nil
}
