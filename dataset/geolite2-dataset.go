package dataset

import (
	"context"
	"errors"
	"log"

	"github.com/m-lab/annotation-service/common"
	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/search"
)

const MaxmindPrefix = "Maxmind/" // Folder containing the maxmind files

// LoadGeoLite2Dataset load the Geolite2 dataset with filename from bucket.
func LoadGeoLite2Dataset(filename string, bucketname string) (*parser.GeoDataset, error) {
	zip, err := loader.CreateZipReader(context.Background(), bucketname, filename)
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

// UseGeoLite2Dataset return annotation for a request from a given Geolite2 dataset.
func UseGeoLite2Dataset(request *common.RequestData, dataset *parser.GeoDataset) (*common.GeoData, error) {
	if dataset == nil {
		// TODO: Block until the value is not nil
		return nil, errors.New("Dataset is not ready")
	}

	err := errors.New("unknown IP format")
	var node parser.IPNode
	// TODO: Push this logic down to searchlist (after binary search is implemented)
	if request.IPFormat == 4 {
		node, err = search.SearchBinary(
			dataset.IP4Nodes, request.IP)
	} else if request.IPFormat == 6 {
		node, err = search.SearchBinary(
			dataset.IP6Nodes, request.IP)
	}

	if err != nil {
		// ErrNodeNotFound is super spammy - 10% of requests, so suppress those.
		if err != search.ErrNodeNotFound {
			log.Println(err, request.IP)
		}
		//TODO metric here
		return nil, err
	}

	return ConvertIPNodeToGeoData(node, dataset.LocationNodes), nil
}
