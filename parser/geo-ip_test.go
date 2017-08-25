package parser_test

import (
	"archive/zip"
	"errors"
	"log"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/parser"
)

func TestInt2ip(t *testing.T) {
	_, err := parser.Int2ip("4294967297")
	if err != nil {
		t.Errorf("Failed to catch out of bounds IP")
	}
}
func TestBadFile(t *testing.T) {
	locationIdMap := map[int]int{
		609013: 0,
		104084: 4,
		17:     4,
	}
	reader, err := zip.OpenReader("testdata/GeoLiteLatest.zip")
	if err != nil {
		t.Errorf("Error opening zip file")
	}
	rc, err := loader.FindFile("GeoLiteCity-Blocks.csv", &reader.Reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	_, err = parser.CreateIPList(rc, locationIdMap, "BADFILE.csv")
	if err.Error() != "Unaccepted csv file provided" {
		t.Errorf("Failed to catch bad csv file")
	}
}
func TestIPGLite1(t *testing.T) {
	var ipv4 []parser.IPNode
	var ipv4Expected = []parser.IPNode{
		parser.IPNode{
			net.ParseIP("1.0.0.0"),
			net.ParseIP("1.0.0.255"),
			0,
			"",
			0,
			0,
		},
		parser.IPNode{
			net.ParseIP("1.0.1.0"),
			net.ParseIP("1.0.3.255"),
			4,
			"",
			0,
			0,
		},
		parser.IPNode{
			net.ParseIP("1.0.4.0"),
			net.ParseIP("1.0.7.255"),
			4,
			"",
			0,
			0,
		},
	}
	locationIdMap := map[int]int{
		609013: 0,
		104084: 4,
		17:     4,
	}
	reader, err := zip.OpenReader("testdata/GeoLiteLatest.zip")
	if err != nil {
		t.Errorf("Error opening zip file")
	}
	rcIPv4, err := loader.FindFile("GeoLiteCity-Blocks.csv", &reader.Reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rcIPv4.Close()
	ipv4, err = parser.CreateIPList(rcIPv4, locationIdMap, "GeoLiteCity-Blocks.csv")
	if err != nil {
		t.Errorf("Failed to create ipv4")
	}
	err = isEqualIPLists(ipv4Expected, ipv4)
	if err != nil {
		t.Errorf("Lists are not equal")
	}
}
func TestIPLisGLite2(t *testing.T) {
	var ipv4, ipv6 []parser.IPNode
	var ipv6Expected = []parser.IPNode{
		parser.IPNode{
			net.ParseIP("600:8801:9400:5a1:948b:ab15:dde3:61a3"),
			net.ParseIP("600:8801:9400:5a1:948b:ab15:dde3:61a3"),
			4,
			"91941",
			32.7596,
			-116.994,
		},
		parser.IPNode{
			net.ParseIP("2001:5::"),
			net.ParseIP("2001:0005:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF"),
			4,
			"",
			47,
			8,
		},
		parser.IPNode{
			net.ParseIP("2001:200::"),
			net.ParseIP("2001:0200:00FF:FFFF:FFFF:FFFF:FFFF:FFFF"),
			4,
			"",
			36,
			138,
		},
	}
	var ipv4Expected = []parser.IPNode{
		parser.IPNode{
			net.ParseIP("1.0.0.0"),
			net.ParseIP("1.0.0.255"),
			0,
			"3095",
			-37.7,
			145.1833,
		},
		parser.IPNode{
			net.ParseIP("1.0.1.0"),
			net.ParseIP("1.0.1.255"),
			4,
			"",
			26.0614,
			119.3061,
		},
		parser.IPNode{
			net.ParseIP("1.0.2.0"),
			net.ParseIP("1.0.3.255"),
			4,
			"",
			26.0614,
			119.3061,
		},
	}

	locationIdMap := map[int]int{
		2151718: 0,
		1810821: 4,
		5363990: 4,
		6255148: 4,
		1861060: 4,
	}
	reader, err := zip.OpenReader("testdata/GeoLite2City.zip")
	if err != nil {
		t.Errorf("Error opening zip file")
	}

	rcIPv4, err := loader.FindFile("GeoLite2-City-Blocks-IPv4.csv", &reader.Reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rcIPv4.Close()
	ipv4, err = parser.CreateIPList(rcIPv4, locationIdMap, "GeoLite2-City-Blocks-IPv4.csv")
	if err != nil {
		t.Errorf("Failed to create ipv4")
	}
	err = isEqualIPLists(ipv4Expected, ipv4)
	if err != nil {
		t.Errorf("Lists are not equal")
	}

	rcIPv6, err := loader.FindFile("GeoLite2-City-Blocks-IPv6.csv", &reader.Reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rcIPv6.Close()
	ipv6, err = parser.CreateIPList(rcIPv6, locationIdMap, "GeoLite2-City-Blocks-IPv6.csv")
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create ipv6")
	}
	err = isEqualIPLists(ipv6Expected, ipv6)
	if err != nil {
		t.Errorf("Lists are not equal")
	}
}

func TestLocationListGLite2(t *testing.T) {
	var locationList []parser.LocationNode
	var idMap map[int]int
	var LocList = []parser.LocationNode{
		parser.LocationNode{
			32909,
			"AS",
			"IR",
			"Iran",
			0,
			"Shahre Jadide Andisheh",
		},
		parser.LocationNode{
			49518,
			"AF",
			"RW",
			"Rwanda",
			0,
			"",
		},
		parser.LocationNode{
			51537,
			"AF",
			"SO",
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

	rc, err := loader.FindFile("GeoLite2-City-Locations-en.csv", &reader.Reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rc.Close()
	locationList, idMap, err = parser.CreateLocationList(rc)
	if err != nil {
		t.Errorf("Failed to CreateLocationList")
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

func isEqualIPLists(listComp, list []parser.IPNode) error {
	for index, element := range list {
		err := parser.IsEqualIPNodes(element, listComp[index])
		if err != nil {
			return err
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
	}
	return nil
}
