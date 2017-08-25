package search_test

import (
	"archive/zip"
	"log"
	"net"
	"testing"

	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/search"
)

func TestSearchGeoLatest(t *testing.T) {
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
	rc, err := loader.FindFile("GeoLiteCity-Blocks.csv", &reader.Reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rc.Close()
	ipv4, err := parser.CreateIPList(rc, locationIdMap, "GeoLiteCity-Blocks.csv")
	if err != nil {
		t.Errorf("Failed to create ipv4")
	}

	ip, err := search.SearchList(ipv4, "1.0.0.0")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	err = parser.IsEqualIPNodes(ipv4Expected[0], ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", ipv4Expected[0])
	}

	ip, err = search.SearchList(ipv4, "1.0.3.254")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	err = parser.IsEqualIPNodes(ipv4Expected[1], ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", ipv4Expected[1])
	}

	// Invalid IP provided
	ip, err = search.SearchList(ipv4, "255.255.255.255")
	if err == nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted : ERROR")
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

	ip, err := search.SearchList(ipv4, "1.0.0.4")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	err = parser.IsEqualIPNodes(ipv4Expected[0], ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", ipv4Expected[0])
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

	ip, err = search.SearchList(ipv6, "2001:5::1")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	err = parser.IsEqualIPNodes(ipv6Expected[1], ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", ipv6Expected[1])
	}

}
