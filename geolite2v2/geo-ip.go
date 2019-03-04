package geolite2v2

import (
	"archive/zip"
	"context"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/iputils"
	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/metrics"
)

const (
	mapMax = 200000
)

var (
	gLite2Prefix              = "GeoLite2-City"
	geoLite2BlocksFilenameIP4 = "GeoLite2-City-Blocks-IPv4.csv"  // Filename of ipv4 blocks file
	geoLite2BlocksFilenameIP6 = "GeoLite2-City-Blocks-IPv6.csv"  // Filename of ipv6 blocks file
	geoLite2LocationsFilename = "GeoLite2-City-Locations-en.csv" // Filename of locations file
)

// The GeoDataset struct bundles all the data needed to search and
// find data into one common structure
// It implements the api.Annotator interface.
type GeoDataset struct {
	Start         time.Time      // Date from which to start using this dataset
	IP4Nodes      []GeoIPNode    // The IPNode list containing IP4Nodes
	IP6Nodes      []GeoIPNode    // The IPNode list containing IP6Nodes
	LocationNodes []LocationNode // The location nodes corresponding to the IPNodes
}

// LoadGeolite2 loads a dataset from a GCS object.
func LoadG2(file *storage.ObjectAttrs) (api.Annotator, error) {
	return LoadG2Dataset(file.Name, file.Bucket)
}

// LoadGeoLite2Dataset loads the dataset from the specified filename and bucket
func LoadG2Dataset(filename string, bucketname string) (*GeoDataset, error) {
	zip, err := loader.CreateZipReader(context.Background(), bucketname, filename)
	log.Println("Loading dataset from", filename)
	if err != nil {
		return nil, err
	}
	dataset, err := loadGeoLite2(zip)
	if err != nil {
		return nil, err
	}
	date, err := api.ExtractDateFromFilename(filename)
	if err != nil {
		log.Println("Error extracting date:", filename)
	} else {
		dataset.Start = date
	}
	return dataset, nil
}

// loadGeoLite2 composes the result of location and IPv4, IPv6 IPNode lists
func loadGeoLite2(zip *zip.Reader) (*GeoDataset, error) {
	locations, err := loader.FindFile(geoLite2LocationsFilename, zip)
	if err != nil {
		return nil, err
	}
	// geoidMap is just a temporary map that will be discarded once the blocks are parsed
	locationNode, geoidMap, err := LoadLocationsG2(locations)
	locations.Close()
	if err != nil {
		return nil, err
	}

	blocks4, err := loader.FindFile(geoLite2BlocksFilenameIP4, zip)
	if err != nil {
		return nil, err
	}
	ipNodes4, err := LoadIPListG2(blocks4, geoidMap)
	blocks4.Close()
	if err != nil {
		return nil, err
	}
	blocks6, err := loader.FindFile(geoLite2BlocksFilenameIP6, zip)
	if err != nil {
		return nil, err
	}
	ipNodes6, err := LoadIPListG2(blocks6, geoidMap)
	blocks6.Close()
	if err != nil {
		return nil, err
	}
	return &GeoDataset{IP4Nodes: ipNodes4, IP6Nodes: ipNodes6, LocationNodes: locationNode}, nil
}

// ConvertIPNodeToGeoData takes a parser.IPNode, plus a list of
// locationNodes. It will then use that data to fill in a GeoData struct.
func populateLocationData(ipNode iputils.IPNode, locationNodes []LocationNode, data *api.GeoData) {
	locNode := LocationNode{}
	geoIPNode := ipNode.(*GeoIPNode)

	if geoIPNode.LocationIndex >= 0 {
		locNode = locationNodes[geoIPNode.LocationIndex]
	}
	data.Geo = &api.GeolocationIP{
		ContinentCode: locNode.ContinentCode,
		CountryCode:   locNode.CountryCode,
		CountryCode3:  "", // missing from geoLite2 ?
		CountryName:   locNode.CountryName,
		Region:        locNode.RegionCode,
		MetroCode:     locNode.MetroCode,
		City:          locNode.CityName,
		AreaCode:      0, // new geoLite2 does not have area code.
		PostalCode:    geoIPNode.PostalCode,
		Latitude:      geoIPNode.Latitude,
		Longitude:     geoIPNode.Longitude,
	}
}

var lastLogTime = time.Time{}

// Annotate annotates the api.GeoData with the location informations
func (ds *GeoDataset) Annotate(ip string, data *api.GeoData) error {
	if data == nil {
		return errors.New("ErrNilGeoData") // TODO
	}
	if data.Geo != nil {
		return errors.New("ErrAlreadyPopulated") // TODO
	}
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		metrics.BadIPTotal.Inc()
		return errors.New("ErrInvalidIP") // TODO
	}
	ipNodes := ds.IP6Nodes
	if parsedIP.To4() != nil {
		ipNodes = ds.IP4Nodes
	}

	node, err := iputils.SearchBinary(ip,
		len(ipNodes),
		func(idx int) iputils.IPNode {
			return &ipNodes[idx]
		})

	if err != nil {
		// ErrNodeNotFound is super spammy - 10% of requests, so suppress those.
		if err != iputils.ErrNodeNotFound {
			// Horribly noisy now.
			if time.Since(lastLogTime) > time.Minute {
				log.Println(err, ip)
				lastLogTime = time.Now()
			}
		}
		//TODO metric here
		return err
	}

	populateLocationData(node, ds.LocationNodes, data)
	return nil
}

// AnnotatorDate returns the date that the dataset was published.
// TODO implement actual dataset time!!
func (ds *GeoDataset) AnnotatorDate() time.Time {
	return ds.Start
}

// IsEqualIPNodes returns nil if two nodes are equal
// Used by the search package
func IsEqualIPNodes(expectedIPNode, ipNode iputils.IPNode) error {
	expected, eok := expectedIPNode.(*GeoIPNode)
	node, nok := ipNode.(*GeoIPNode)
	if !eok || !nok {
		return errors.New("Illegal type of IPNode")
	}

	if !((node.IPAddressLow).Equal(expected.IPAddressLow)) {
		output := strings.Join([]string{"IPAddress Low inconsistent\ngot:", node.IPAddressLow.String(), " \nwanted:", expected.IPAddressLow.String()}, "")
		log.Println(output)
		return errors.New(output)
	}
	if !((node.IPAddressHigh).Equal(expected.IPAddressHigh)) {
		output := strings.Join([]string{"IPAddressHigh inconsistent\ngot:", node.IPAddressHigh.String(), " \nwanted:", expected.IPAddressHigh.String()}, "")
		log.Println(output)
		return errors.New(output)
	}
	if node.LocationIndex != expected.LocationIndex {
		output := strings.Join([]string{"LocationIndex inconsistent\ngot:", strconv.Itoa(node.LocationIndex), " \nwanted:", strconv.Itoa(expected.LocationIndex)}, "")
		log.Println(output)
		return errors.New(output)
	}
	if node.PostalCode != expected.PostalCode {
		output := strings.Join([]string{"PostalCode inconsistent\ngot:", node.PostalCode, " \nwanted:", expected.PostalCode}, "")
		log.Println(output)
		return errors.New(output)
	}
	if node.Latitude != expected.Latitude {
		output := strings.Join([]string{"Latitude inconsistent\ngot:", floatToString(node.Latitude), " \nwanted:", floatToString(expected.Latitude)}, "")
		log.Println(output)
		return errors.New(output)
	}
	if node.Longitude != expected.Longitude {
		output := strings.Join([]string{"Longitude inconsistent\ngot:", floatToString(node.Longitude), " \nwanted:", floatToString(expected.Longitude)}, "")
		log.Println(output)
		return errors.New(output)
	}
	return nil
}

func floatToString(num float64) string {
	return strconv.FormatFloat(num, 'f', 6, 64)
}