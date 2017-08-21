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
	"github.com/m-lab/annotation-service/loader"
)

func TestIPList(t *testing.T) {
	var ipv4 []parser.IPNode
	var ipv4Expected = []parser.IPNode{
		parser.IPNode{
			"1.0.0.0/24",
			0,
			"3095",
			-37.7,
			145.1833,
		},
		parser.IPNode{
			"1.0.1.0/24",
			4,
			"",
			26.0614,
			119.3061,
		},
		parser.IPNode{
			"1.0.2.0/23",
			4,
			"",
			26.0614,
			119.3061,
		},
	}
	locationIdMap := map[int]int{
		2151718: 0,
		1810821: 4,
	}
	reader, err := zip.OpenReader("testdata/GeoLite2City.zip")
	if err != nil {
		t.Errorf("Error opening zip file")
	}
	rc, err := loader.FindFile("GeoLite2-City-Blocks-IPv4.csv", &reader.Reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rc.Close()
	ipv4, err = parser.CreateIPList(rc, locationIdMap)
	if err != nil {
		t.Errorf("Failed to create ipv4")
	}
	err = compareIPLists(ipv4Expected, ipv4)
	if err != nil {
		t.Errorf("Lists are not equal")
	}

}

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
			if err.Error() != "Corrupted Data: wrong number of columns" {
				if err == nil {
					t.Errorf("Error inconsistent:\ngot: nil\nwanted: Corrupted Data: wrong number of columns")
				}
				if err != nil {
					output := strings.Join([]string{"Error inconsistent:\ngot: ", err.Error(), "\nwanted: Corrupted Data: wrong number of columns"}, "")
					t.Errorf(output)
				}

			}
		}
	}

}

func compareIPLists(list, listComp []parser.IPNode) error {
	for index, element := range list {
		if element.IPAddress != listComp[index].IPAddress {
			output := strings.Join([]string{"IPAddress inconsistent\ngot:", element.IPAddress, " \nwanted:", listComp[index].IPAddress}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.LocationIndex != listComp[index].LocationIndex {
			output := strings.Join([]string{"LocationIndex inconsistent\ngot:", strconv.Itoa(element.LocationIndex), " \nwanted:", strconv.Itoa(listComp[index].LocationIndex)}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.PostalCode != listComp[index].PostalCode {
			output := strings.Join([]string{"PostalCode inconsistent\ngot:", element.PostalCode, " \nwanted:", listComp[index].PostalCode}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.Latitude != listComp[index].Latitude {
			output := strings.Join([]string{"Latitude inconsistent\ngot:", strconv.FormatFloat(element.Latitude, 'f', 6, 64), " \nwanted:", strconv.FormatFloat(listComp[index].Latitude, 'f', 6, 64)}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.Longitude != listComp[index].Longitude {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", strconv.FormatFloat(element.Longitude, 'f', 6, 64), " \nwanted:", strconv.FormatFloat(listComp[index].Longitude, 'f', 6, 64)}, "")
			log.Println(output)
			return errors.New(output)
		}

	}
	return nil
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
