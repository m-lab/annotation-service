package parser_test

import (
	"archive/zip"
	"errors"
	"strconv"
	"strings"
	"testing"
	"log"

	"github.com/m-lab/annotation-service/parser"
)

func TestUnzip(t *testing.T) {
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
	var LocIdMap := map[int]int{
		0 : 32909,
		1 : 49518,
		2 : 51537,
	} 

	reader, err := zip.OpenReader("testdata/GeoLite2City.zip")
	if err != nil {
		t.Errorf("Error opening zip file")
	}
	r := &(reader.Reader)
	for _,f := range r.File {
		if len(f.Name) >= len("GeoLite2-City-Locations-en.csv") && f.Name[len(f.Name)-len("GeoLite2-City-Locations-en.csv"):] == "GeoLite2-City-Locations-en.csv"{
			rc, err := f.Open()
			if err != nil {
				t.Errorf("Failed to open GeoLite2-City-Locations-en.csv")
			}
			defer rc.Close()
			locationList, idMap, err = parser.CreateLocationList(rc)
			if err != nil {
				log.Println(err) 
			}
			break
		}
	}
	if locationList == nil || idMap == nil {
		t.Errorf("Failed to create LocationList and mapID") 
	}

	err = compareLocLists(locationList,LocList)
	if err != nil {
		t.Errorf("Location lists are not equal")
	}
}

func compareLocLists(list, listComp []parser.LocationNode) error {
	for index, element := range list {
		if element.Geoname != listComp[index].Geoname {
			output := strings.Join([]string{"Geoname inconsistent\ngot:", strconv.Itoa(element.Geoname), " \nwanted:", strconv.Itoa(listComp[index].Geoname)}, "")
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
