package handler_test

import (
	"reflect"
	"testing"

	"github.com/m-lab/annotation-service/common"
	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/parser"
)

func TestConvertIPNodeToGeoData(t *testing.T) {
	tests := []struct {
		node parser.IPNode
		locs []parser.LocationNode
		res  *common.GeoData
	}{
		{
			node: parser.IPNode{LocationIndex: 0, PostalCode: "10583"},
			locs: []parser.LocationNode{{CityName: "Not A Real City", RegionCode: "ME"}},
			res: &common.GeoData{
				Geo: &common.GeolocationIP{City: "Not A Real City", Postal_code: "10583", Region: "ME"},
				ASN: &common.IPASNData{}},
		},
		{
			node: parser.IPNode{LocationIndex: -1, PostalCode: "10583"},
			locs: nil,
			res: &common.GeoData{
				Geo: &common.GeolocationIP{Postal_code: "10583"},
				ASN: &common.IPASNData{}},
		},
	}
	for _, test := range tests {
		res := handler.ConvertIPNodeToGeoData(test.node, test.locs)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %v, got %v", test.res, res)
		}
	}
}
