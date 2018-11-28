package parser_test

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/parser"
)

// Returns nil if two IP Lists are equal
func isEqualIPLists(listComp, list []parser.IPNode) error {
	for index, element := range list {
		err := parser.IsEqualIPNodes(element, listComp[index])
		if err != nil {
			return err
		}
	}
	return nil
}

// Returns nil if two Location lists are equal
func isEqualLocLists(list, listComp []parser.LocationNode) error {
	for index, element := range list {
		if index >= len(listComp) {
			output := fmt.Sprint("Out of range:", index)
			log.Println(output)
			return errors.New(output)
		}
		if element.GeonameID != listComp[index].GeonameID {
			output := strings.Join([]string{"GeonameID inconsistent\ngot:", strconv.Itoa(element.GeonameID), " \nwanted:", strconv.Itoa(listComp[index].GeonameID)}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.ContinentCode != listComp[index].ContinentCode {
			output := strings.Join([]string{"Continent code inconsistent\ngot:", element.ContinentCode, " \nwanted:", listComp[index].ContinentCode}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.CountryCode != listComp[index].CountryCode {
			output := strings.Join([]string{"Country code inconsistent\ngot:", element.CountryCode, " \nwanted:", listComp[index].CountryCode}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.CountryName != listComp[index].CountryName {
			output := strings.Join([]string{"Country name inconsistent\ngot:", element.CountryName, " \nwanted:", listComp[index].CountryName}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.MetroCode != listComp[index].MetroCode {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", strconv.FormatInt(element.MetroCode, 16), " \nwanted:", strconv.FormatInt(listComp[index].MetroCode, 16)}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.CityName != listComp[index].CityName {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", element.CityName, " \nwanted:", listComp[index].CityName}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.RegionName != listComp[index].RegionName {
			output := strings.Join([]string{"RegionName inconsistent\ngot:", element.RegionName, " \nwanted:", listComp[index].RegionName}, "")
			log.Println(output)
			return errors.New(output)
		}
	}
	return nil
}

func TestConvertIPNodeToGeoData(t *testing.T) {
	tests := []struct {
		node parser.IPNode
		locs []parser.LocationNode
		res  *api.GeoData
	}{
		{
			node: parser.IPNode{LocationIndex: 0, PostalCode: "10583"},
			locs: []parser.LocationNode{{CityName: "Not A Real City", RegionCode: "ME"}},
			res: &api.GeoData{
				Geo: &api.GeolocationIP{City: "Not A Real City", Postal_code: "10583", Region: "ME"},
				ASN: &api.IPASNData{}},
		},
		{
			node: parser.IPNode{LocationIndex: -1, PostalCode: "10583"},
			locs: nil,
			res: &api.GeoData{
				Geo: &api.GeolocationIP{Postal_code: "10583"},
				ASN: &api.IPASNData{}},
		},
	}
	for _, test := range tests {
		res := parser.ConvertIPNodeToGeoData(test.node, test.locs)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %v, got %v", test.res, res)
		}
	}
}
