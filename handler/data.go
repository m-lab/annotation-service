package handler

import (
	"context"
	"os"
	"regexp"

	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/parser"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

type GeoDataset struct {
	IP4Nodes      []parser.IPNode
	IP6Nodes      []parser.IPNode
	LocationNodes []parser.LocationNode
	GeoidMap      map[int]int
}

var GeoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)

var BucketName = "downloader-" + os.Getenv("GCLOUD_PROJECT")

const (
	MaxmindPrefix             = "Maxmind/"
	GeoLite2BlocksFilenameIP4 = "GeoLite2-City-Blocks-IPv4.csv"
	GeoLite2BlocksFilenameIP6 = "GeoLite2-City-Blocks-IPv6.csv"
	GeoLite2LocationsFilename = "GeoLite2-City-Locations-en.csv"
)

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
	return &GeoDataset{IP4Nodes: ipNodes4, IP6Nodes: ipNodes6, LocationNodes: locationNodes, GeoidMap: geoidMap}, nil
}
