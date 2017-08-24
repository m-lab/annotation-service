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
	err = compareIPLists(ipv4Expected, ipv4)
	if err != nil {
		t.Errorf("Lists are not equal")
	}
}
func TestIPLisGLite2(t *testing.T) {
	var ipv4, ipv6 []parser.IPNode
	var ipv6Expected = []parser.IPNode{
		parser.IPNode{
			parser.RangeCIDR("600:8801:9400:5a1:948b:ab15:dde3:61a3/128", "low"),
			parser.RangeCIDR("600:8801:9400:5a1:948b:ab15:dde3:61a3/128", "high"),
			4,
			"91941",
			32.7596,
			-116.994,
		},
		parser.IPNode{
			parser.RangeCIDR("2001:5::/32", "low"),
			parser.RangeCIDR("2001:5::/32", "high"),
			4,
			"",
			47,
			8,
		},
		parser.IPNode{
			parser.RangeCIDR("2001:200::/40", "low"),
			parser.RangeCIDR("2001:200::/40", "high"),
			4,
			"",
			36,
			138,
		},
	}
	var ipv4Expected = []parser.IPNode{
		parser.IPNode{
			parser.RangeCIDR("1.0.0.0/24", "low"),
			parser.RangeCIDR("1.0.0.0/24", "high"),
			0,
			"3095",
			-37.7,
			145.1833,
		},
		parser.IPNode{
			parser.RangeCIDR("1.0.1.0/24", "low"),
			parser.RangeCIDR("1.0.1.0/24", "high"),
			4,
			"",
			26.0614,
			119.3061,
		},
		parser.IPNode{
			parser.RangeCIDR("1.0.2.0/23", "low"),
			parser.RangeCIDR("1.0.2.0/23", "high"),
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
	ipv4, err = parser.CreateIPList(rcIPv4, locationIdMap, "geolite2")
	if err != nil {
		t.Errorf("Failed to create ipv4")
	}
	err = compareIPLists(ipv4Expected, ipv4)
	if err != nil {
		t.Errorf("Lists are not equal")
	}

	rcIPv6, err := loader.FindFile("GeoLite2-City-Blocks-IPv6.csv", &reader.Reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rcIPv6.Close()
	ipv6, err = parser.CreateIPList(rcIPv6, locationIdMap, "geolite2")
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create ipv6")
	}
	err = compareIPLists(ipv6Expected, ipv6)
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

func floatToString(num float64) string {
	return strconv.FormatFloat(num, 'f', 6, 64)
}

func compareIPLists(listComp, list []parser.IPNode) error {
	for index, element := range list {
		if !((element.IPAddressLow).Equal(listComp[index].IPAddressLow)) {
			output := strings.Join([]string{"IPAddress Low inconsistent\ngot:", element.IPAddressLow.String(), " \nwanted:", listComp[index].IPAddressLow.String()}, "")
			log.Println(output)
			return errors.New(output)
		}
		if !((element.IPAddressHigh).Equal(listComp[index].IPAddressHigh)) {
			output := strings.Join([]string{"IPAddressHigh inconsistent\ngot:", element.IPAddressHigh.String(), " \nwanted:", listComp[index].IPAddressHigh.String()}, "")
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
			output := strings.Join([]string{"Latitude inconsistent\ngot:", floatToString(element.Latitude), " \nwanted:", floatToString(listComp[index].Latitude)}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.Longitude != listComp[index].Longitude {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", floatToString(element.Longitude), " \nwanted:", floatToString(listComp[index].Longitude)}, "")
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
