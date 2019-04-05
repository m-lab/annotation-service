package geolite2v2_test

// TODO - migrate these tests to geolite2v2 before removing geolite2 package

import (
	"archive/zip"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"testing"

	"github.com/go-test/deep"

	"github.com/m-lab/annotation-service/geolite2v2"
	"github.com/m-lab/annotation-service/loader"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Returns nil if two IP Lists are equal
func isEqualIPLists(listComp, list []geolite2v2.GeoIPNode) error {
	for index, element := range list {
		err := geolite2v2.IsEqualIPNodes(&element, &listComp[index])
		if err != nil {
			return err
		}
	}
	return nil
}

// Returns nil if two Location lists are equal
func isEqualLocLists(list, listComp []geolite2v2.LocationNode) error {
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

/*
func TestIPLisGLite2(t *testing.T) {
	var ipv4, ipv6 []geolite2v2.GeoIPNode
	var ipv6Expected = []geolite2v2.GeoIPNode{
		{
			IPAddressLow:  net.ParseIP("600:8801:9400:5a1:948b:ab15:dde3:61a3"),
			IPAddressHigh: net.ParseIP("600:8801:9400:5a1:948b:ab15:dde3:61a3"),
			LocationIndex: 4,
			PostalCode:    "91941",
			Latitude:      32.7596,
			Longitude:     -116.994,
		},
		{
			IPAddressLow:  net.ParseIP("2001:5::"),
			IPAddressHigh: net.ParseIP("2001:0005:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF"),
			LocationIndex: 4,
			Latitude:      47,
			Longitude:     8,
		},
		{
			IPAddressLow:  net.ParseIP("2001:200::"),
			IPAddressHigh: net.ParseIP("2001:0200:00FF:FFFF:FFFF:FFFF:FFFF:FFFF"),
			LocationIndex: 4,
			Latitude:      36,
			Longitude:     138,
		},
	}
	var ipv4Expected = []geolite2v2.GeoIPNode{
		{
			IPAddressLow:  net.ParseIP("1.0.0.0"),
			IPAddressHigh: net.ParseIP("1.0.0.255"),
			LocationIndex: 0,
			PostalCode:    "3095",
			Latitude:      -37.7,
			Longitude:     145.1833,
		},
		{
			IPAddressLow:  net.ParseIP("1.0.1.0"),
			IPAddressHigh: net.ParseIP("1.0.1.255"),
			LocationIndex: 4,
			Latitude:      26.0614,
			Longitude:     119.3061,
		},
		{
			IPAddressLow:  net.ParseIP("1.0.2.0"),
			IPAddressHigh: net.ParseIP("1.0.3.255"),
			LocationIndex: 4,
			Latitude:      26.0614,
			Longitude:     119.3061,
		},
	}

	locationIDMap := map[int]int{
		2151718: 0,
		1810821: 4,
		5363990: 4,
		6255148: 4,
		1861060: 4,
	}
	reader, err := zip.OpenReader("testdata/GeoLite2City.zip")
	if err != nil {
		t.Fatalf("Error opening zip file")
	}

	rcIPv4, err := loader.FindFile("GeoLite2-City-Blocks-IPv4.csv", &reader.Reader)
	if err != nil {
		t.Fatalf("Failed to create io.ReaderCloser")
	}
	defer rcIPv4.Close()
	ipv4, err = geolite2.LoadIPListGLite2(rcIPv4, locationIDMap)
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
	ipv6, err = geolite2.LoadIPListGLite2(rcIPv6, locationIDMap)
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create ipv6")
	}
	err = isEqualIPLists(ipv6Expected, ipv6)
	if err != nil {
		t.Errorf("Lists are not equal")
	}
}
*/
func TestLocationListGLite2(t *testing.T) {
	var expectedLocList = []geolite2v2.LocationNode{
		{
			GeonameID:     32909,
			ContinentCode: "AS",
			CountryCode:   "IR",
			CountryName:   "Iran",
			RegionCode:    "07",
			RegionName:    "Ostan-e Tehran",
			MetroCode:     0,
			CityName:      "Shahre Jadide Andisheh",
		},
		{
			GeonameID:     49518,
			ContinentCode: "AF",
			CountryCode:   "RW",
			CountryName:   "Rwanda",
		},
		{
			GeonameID:     51537,
			ContinentCode: "AF",
			CountryCode:   "SO",
			CountryName:   "Somalia",
		},
		{
			GeonameID:     5127766,
			ContinentCode: "NA",
			CountryCode:   "US",
			CountryName:   "United States",
			RegionCode:    "NY",
			RegionName:    "New York",
			MetroCode:     538,
			CityName:      "Mount Morris",
		},
	}
	expectedIDMap := map[int]int{
		5127766: 3,
		51537:   2,
		49518:   1,
		32909:   0,
	}

	reader, err := zip.OpenReader("testdata/GeoLite2City.zip")
	if err != nil {
		t.Fatalf("Error opening zip file")
	}

	rc, err := loader.FindFile("GeoLite2-City-Locations-en.csv", &reader.Reader)
	if err != nil {
		t.Fatalf("Failed to create io.ReaderCloser")
	}
	defer rc.Close()

	var actualLocList []geolite2v2.LocationNode
	var actualIDMap map[int]int
	actualLocList, actualIDMap, err = geolite2v2.LoadLocationsG2(rc)
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to LoadLocationList")
	}
	if actualLocList == nil || actualIDMap == nil {
		t.Errorf("Failed to create LocationList and mapID")
	}

	if diff := deep.Equal(actualLocList, expectedLocList); diff != nil {
		log.Printf("%+v\n", actualLocList)
		log.Printf("%+v\n", expectedLocList)
		t.Error(diff)
	}
	err = isEqualLocLists(actualLocList, expectedLocList)
	if err != nil {
		t.Errorf("Location lists are not equal")
	}

	if diff := deep.Equal(expectedIDMap, actualIDMap); diff != nil {
		t.Error(diff)
	}
}

func TestCorruptData(t *testing.T) {
	reader, err := zip.OpenReader("testdata/GeoLite2CityCORRUPT.zip")
	if err != nil {
		t.Fatalf("Error opening zip file")
	}
	rc, err := loader.FindFile("GeoLite2-City-Locations-en.csv", &reader.Reader)
	if err != nil {
		t.Fatalf("Error finding file")
	}
	_, _, err = geolite2v2.LoadLocationsG2(rc)
	if err == nil {
		t.Error("Should have errored")
	} else if err.Error() != "Corrupted Data: wrong number of columns" {
		if err == nil {
			t.Errorf("Error inconsistent:\ngot: nil\nwanted: Corrupted Data: wrong number of columns")
		}
		if err != nil {
			output := strings.Join([]string{"Error inconsistent:\ngot:", err.Error(), "\nwanted: Corrupted Data: wrong number of columns"}, "")
			t.Errorf(output)
		}
	}
}
