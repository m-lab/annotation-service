package geolite2v2_test

// TODO - migrate these tests to geolite2v2 before removing geolite2 package

import (
	"archive/zip"
	"log"
	"net"
	"strings"
	"testing"

	"github.com/go-test/deep"

	"github.com/m-lab/annotation-service/geolite2v2"
	"github.com/m-lab/annotation-service/iputils"
	"github.com/m-lab/annotation-service/loader"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestIPListGLite2v4(t *testing.T) {
	expect := []geolite2v2.GeoIPNode{
		{
			BaseIPNode: iputils.BaseIPNode{
				IPAddressLow:  net.ParseIP("1.0.0.0"),
				IPAddressHigh: net.ParseIP("1.0.0.255")},
			LocationIndex: 0,
			PostalCode:    "3095",
			Latitude:      -37.7,
			Longitude:     145.1833,
		},
		{
			BaseIPNode: iputils.BaseIPNode{
				IPAddressLow:  net.ParseIP("1.0.1.0"),
				IPAddressHigh: net.ParseIP("1.0.3.255")}, // BUG: Instead we are getting 1.0.1.255
			LocationIndex: 4,
			Latitude:      26.0614,
			Longitude:     119.3061,
		},
	}

	// Guess this is a fake map.  Why?
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

	csv, err := loader.FindFile("GeoLite2-City-Blocks-IPv4.csv", &reader.Reader)
	if err != nil {
		t.Fatalf("Failed to create io.ReaderCloser")
	}
	defer csv.Close()
	got, err := geolite2v2.LoadIPListG2(csv, locationIDMap)
	if err != nil {
		t.Errorf("Failed to create ipv4")
	}
	if len(expect) != len(got) {
		t.Errorf("wrong number of nodes. Expected: %d. Got %d.\n", len(expect), len(got))
		t.Logf("Expected:\n%+v\n", expect)
		t.Logf("Got:\n%+v\n", got)
	} else if diff := deep.Equal(expect, got); diff != nil {
		t.Error(diff)
	}
}

func TestIPListGLite2v6(t *testing.T) {
	// Guess this is a fake map.  Why?
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

	expect := []geolite2v2.GeoIPNode{
		{
			BaseIPNode: iputils.BaseIPNode{
				IPAddressLow:  net.ParseIP("600:8801:9400:5a1:948b:ab15:dde3:61a3"),
				IPAddressHigh: net.ParseIP("600:8801:9400:5a1:948b:ab15:dde3:61a3")},
			LocationIndex: 4,
			PostalCode:    "91941",
			Latitude:      32.7596,
			Longitude:     -116.994,
		},
		{
			BaseIPNode: iputils.BaseIPNode{
				IPAddressLow:  net.ParseIP("2001:5::"),
				IPAddressHigh: net.ParseIP("2001:0005:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF")},
			LocationIndex: 4,
			Latitude:      47,
			Longitude:     8,
		},
		{
			BaseIPNode: iputils.BaseIPNode{
				IPAddressLow:  net.ParseIP("2001:200::"),
				IPAddressHigh: net.ParseIP("2001:0200:00FF:FFFF:FFFF:FFFF:FFFF:FFFF")},
			LocationIndex: 4,
			Latitude:      36,
			Longitude:     138,
		},
	}
	csv, err := loader.FindFile("GeoLite2-City-Blocks-IPv6.csv", &reader.Reader)
	if err != nil {
		t.Fatalf("Failed to create io.ReaderCloser")
	}
	defer csv.Close()
	got, err := geolite2v2.LoadIPListG2(csv, locationIDMap)
	if err != nil {
		t.Errorf("Failed to create ipv6")
	}
	if len(expect) != len(got) {
		t.Errorf("wrong number of nodes. Expected: %d. Got %d.\n", len(expect), len(got))
		t.Logf("Expected:\n%+v\n", expect)
		t.Logf("Got:\n%+v\n", got)
	} else if diff := deep.Equal(expect, got); diff != nil {
		t.Error(diff)
	}
}

func TestLocationListGLite2(t *testing.T) {
	expectedLocList := []geolite2v2.LocationNode{
		{
			GeonameID:           32909,
			ContinentCode:       "AS",
			CountryCode:         "IR",
			CountryName:         "Iran",
			Subdivision1ISOCode: "07",
			Subdivision1Name:    "Ostan-e Tehran",
			MetroCode:           0,
			CityName:            "Shahre Jadide Andisheh",
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
			GeonameID:           5127766,
			ContinentCode:       "NA",
			CountryCode:         "US",
			CountryName:         "United States",
			Subdivision1ISOCode: "NY",
			Subdivision1Name:    "New York",
			MetroCode:           538,
			CityName:            "Mount Morris",
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

	actualLocList, actualIDMap, err := geolite2v2.LoadLocationsG2(rc)
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to LoadLocationList")
	}

	if diff := deep.Equal(expectedLocList, actualLocList); diff != nil {
		log.Printf("Expected%+v\n", expectedLocList)
		log.Printf("Actual:%+v\n", actualLocList)
		t.Error(diff)
	}
	if diff := deep.Equal(expectedIDMap, actualIDMap); diff != nil {
		log.Printf("Expected%+v\n", expectedIDMap)
		log.Printf("Actual:%+v\n", actualIDMap)
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
