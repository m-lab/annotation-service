package parser_test

import (
	"archive/zip"
	"log"
	"net"
	"reflect"
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
			1,
			"",
			35,
			105,
		},
		parser.IPNode{
			net.ParseIP("1.0.1.0"),
			net.ParseIP("1.0.3.255"),
			2,
			"",
			47,
			8,
		},
		parser.IPNode{
			net.ParseIP("1.0.4.0"),
			net.ParseIP("1.0.7.255"),
			0,
			"",
			0,
			0,
		},
	}
	locationIdMap := map[int]int{
		17:     0,
		609013: 1,
		104084: 2,
	}
	reader, err := zip.OpenReader("testdata/GeoLiteLatest.zip")
	if err != nil {
		t.Errorf("Error opening zip file")
	}
	rcloc, err := loader.FindFile("GeoLiteCity-Location.csv", &reader.Reader)
	_, glitehelp, _, err := parser.LoadLocListGLite1(rcloc)
	rcIPv4, err := loader.FindFile("GeoLiteCity-Blocks.csv", &reader.Reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rcIPv4.Close()
	ipv4, err = parser.LoadIPListGLite1(rcIPv4, locationIdMap, glitehelp)
	if err != nil {
		t.Errorf("Failed to create ipv4")
	}
	err = isEqualIPLists(ipv4Expected, ipv4)
	if err != nil {
		t.Errorf("Lists are not equal")
	}
}

func TestLocationListGLite1(t *testing.T) {
	var locationList []parser.LocationNode  
	var idMap map[int]int
	var LocList = []parser.LocationNode  {
		parser.LocationNode  {
			1,
			"",
			"O1",
			"",
			0,
			"",
		},
		parser.LocationNode  {
			2,
			"",
			"AP",
			"",
			0,
			"",
		},
		parser.LocationNode  {
			3,
			"",
			"EU",
			"",
			0,
			"",
		},
	}
	LocIdMap := map[int]int{
		3: 2,
		2: 1,
		1: 0,
	}
	reader, err := zip.OpenReader("testdata/GeoLiteLatest.zip")
	if err != nil {
		t.Errorf("Error opening zip file")
	}

	rc, err := loader.FindFile("GeoLiteCity-Location.csv", &reader.Reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rc.Close()
	locationList, _, idMap, err = parser.LoadLocListGLite1(rc)
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to CreateLocationList")
	}
	if locationList == nil || idMap == nil {
		t.Errorf("Failed to create LocationList and mapID")
	}

	err = isEqualLocLists(locationList, LocList)
	if err != nil {
		t.Errorf("Location lists are not equal")
	}

	eq := reflect.DeepEqual(LocIdMap, idMap)
	if !eq {
		t.Errorf("Location maps are not equal")
	}
}
