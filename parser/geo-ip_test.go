package parser_test

import (
	"archive/zip"
	"errors"
	"log"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/m-lab/annotation-service/parser"
)

func TestLocationList(t *testing.T) {
	var locationList []parser.LocationNode
	var idMap map[int]int
	var LocList = []parser.LocationNode{
		parser.LocationNode{
			32909,
			"AS",
			"Iran",
			0,
			"Shahre Jadide Andisheh",
		},
		parser.LocationNode{
			49518,
			"AF",
			"Rwanda",
			0,
			"",
		},
		parser.LocationNode{
			51537,
			"AF",
			"Somalia",
			0,
			"",
		},
	}
	LocIdMap := map[int]int{
		51537: 2,
		49518: 1,
		32909: 0,
	}

	reader, err := zip.OpenReader("testdata/GeoLite2City.zip")
	if err != nil {
		t.Errorf("Error opening zip file")
	}
	r := &(reader.Reader)
	for _, f := range r.File {
		if len(f.Name) >= len("GeoLite2-City-Locations-en.csv") && f.Name[len(f.Name)-len("GeoLite2-City-Locations-en.csv"):] == "GeoLite2-City-Locations-en.csv" {
			rc, err := f.Open()
			if err != nil {
				t.Errorf("Failed to open GeoLite2-City-Locations-en.csv")
			}
			defer rc.Close()
			locationList, idMap, err = parser.CreateLocationList(rc)
			if err != nil {
				t.Errorf("Failed to create Location list")
			}
			break
		}
	}
	if locationList == nil || idMap == nil {
		t.Errorf("Failed to create LocationList and mapID")
	}

	err = compareLocLists(locationList, LocList)
	if err != nil {
		t.Errorf("Location lists are not equal")
	}

	eq := reflect.DeepEqual(LocIdMap, idMap)
	if !eq {
		t.Errorf("Location maps are not equal")
	}
}

func TestCorruptData(t *testing.T) {
	reader, err := zip.OpenReader("testdata/GeoLite2CityCORRUPT.zip")
	if err != nil {
		t.Errorf("Error opening zip file")
	}
	r := &(reader.Reader)
	for _, f := range r.File {
		if len(f.Name) >= len("GeoLite2-City-Locations-en.csv") && f.Name[len(f.Name)-len("GeoLite2-City-Locations-en.csv"):] == "GeoLite2-City-Locations-en.csv" {
			rc, err := f.Open()
			if err != nil {
				t.Errorf("Failed to open GeoLite2-City-Locations-en.csv")
			}
			defer rc.Close()
			_, _, err = parser.CreateLocationList(rc)
			if err == nil {
				t.Errorf("Failed to recognize missing rows")
			}
			break
		}
	}

}

func compareLocLists(list, listComp []parser.LocationNode) error {
	for index, element := range list {
		if element.GeonameID != listComp[index].GeonameID {
			output := strings.Join([]string{"GeonameID inconsistent\ngot:", strconv.Itoa(element.GeonameID), " \nwanted:", strconv.Itoa(listComp[index].GeonameID)}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.ContinentCode != listComp[index].ContinentCode {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", element.ContinentCode, " \nwanted:", listComp[index].ContinentCode}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.CountryName != listComp[index].CountryName {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", element.CountryName, " \nwanted:", listComp[index].CountryName}, "")
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
	}
	return nil
}
