package search_test

import (
	"log"
	//"net"
	"testing"

	"google.golang.org/appengine/aetest"

	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/parser"
	//"github.com/m-lab/annotation-service/search"
)

/*func TestSearchSmallRange(t *testing.T) {
	var ipv4 = []parser.IPNode{
		parser.IPNode{
			net.ParseIP("1.0.0.0"),
			net.ParseIP("1.0.0.255"),
			0,
			"",
			0,
			0,
		},
		parser.IPNode{
			net.ParseIP("1.0.0.2"),
			net.ParseIP("1.0.0.200"),
			4,
			"",
			0,
			0,
		},
		parser.IPNode{
			net.ParseIP("1.0.0.5"),
			net.ParseIP("1.0.0.100"),
			0,
			"",
			0,
			0,
		},
		parser.IPNode{
			net.ParseIP("1.0.0.120"),
			net.ParseIP("1.0.0.140"),
			0,
			"",
			0,
			0,
		},
		parser.IPNode{
			net.ParseIP("1.0.0.121"),
			net.ParseIP("1.0.0.125"),
			0,
			"",
			0,
			0,
		},
		parser.IPNode{
			net.ParseIP("1.0.0.129"),
			net.ParseIP("1.0.0.130"),
			0,
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

	// Test IP node within several subsets
	ip, err := search.SearchList(ipv4, "1.0.0.122")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	err = parser.IsEqualIPNodes(ipv4[4], ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", ipv4[4])
	}

	// Test IP node not in a subset
	ip, err = search.SearchList(ipv4, "1.0.0.254")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	err = parser.IsEqualIPNodes(ipv4[0], ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", ipv4[0])
	}

	// Test first IP node
	ip, err = search.SearchList(ipv4, "1.0.0.254")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	err = parser.IsEqualIPNodes(ipv4[0], ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", ipv4[0])
	}

	// Test last IP node in the list
	ip, err = search.SearchList(ipv4, "1.0.6.0")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	err = parser.IsEqualIPNodes(ipv4[6], ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", ipv4[6])
	}

	// Test IP NOT in list
	ip, err = search.SearchList(ipv4, "255.0.6.0")
	if err == nil {
		log.Println("Got ", ip, " wanted: Node not found")
		t.Errorf("Search failed")
	}
}*/

func TestGeoLiteLatest(t *testing.T){
	ctx, done, err := aetest.NewContext()
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create aecontext")
	}
	defer done()
	reader, err := loader.CreateZipReader(ctx, "test-annotator-sandbox", "MaxMind/2017/08/08/GeoLiteLatest.zip")
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create zipReader")
	}

	// Create Location list
	rc, err := loader.FindFile("GeoLiteCity-Location.csv", reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rc.Close()

	locationList, idMap, err := parser.CreateLocationList(rc)
	if err != nil {
		t.Errorf("Failed to CreateLocationList")
	}
	if locationList == nil || idMap == nil {
		t.Errorf("Failed to create LocationList and mapID")
	}

	rcIPv4, err := loader.FindFile("GeoLiteCity-Blocks.csv", reader)
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rcIPv4.Close()
}

/*func TestGeoLite2(t *testing.T) {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create aecontext")
	}
	defer done()
	reader, err := loader.CreateZipReader(ctx, "test-annotator-sandbox", "MaxMind/2017/08/08/GeoLite2.zip")
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create zipReader")
	}

	// Create Location list
	rc, err := loader.FindFile("GeoLite2-City-Locations-en.csv", reader)
	if err != nil {
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rc.Close()

	locationList, idMap, err := parser.CreateLocationList(rc)
	if err != nil {
		t.Errorf("Failed to CreateLocationList")
	}
	if locationList == nil || idMap == nil {
		t.Errorf("Failed to create LocationList and mapID")
	}

	// Test IPv6
	rcIPv6, err := loader.FindFile("GeoLite2-City-Blocks-IPv6.csv", reader)
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rcIPv6.Close()

	ipv6, err := parser.CreateIPList(rcIPv6, idMap, "GeoLite2-City-Blocks-IPv6.csv")
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create ipv4")
	}
	ip, err := search.SearchList(ipv6, "2A02:0C7D:5DB7:0000:0000:FFFF:0000:0000")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	var n = parser.IPNode{
		net.ParseIP("2A02:0C7D:5DB7:0000:0000:0000:0000:0000"),
		net.ParseIP("2A02:0C7D:5DB7:FFFF:FFFF:FFFF:FFFF:FFFF"),
		20548,
		"IP1",
		52.0713,
		1.1444,
	}
	err = parser.IsEqualIPNodes(n, ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", n)
	}

	ip, err = search.SearchList(ipv6, "2A04:AB87:FFFF:FFFF:FFFF:FFFF:FFFF:0000")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	n = parser.IPNode{
		net.ParseIP("2A04:AB80:0000:0000:0000:0000:0000:0000"),
		net.ParseIP("2A04:AB87:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF"),
		26082,
		"",
		52.5,
		5.75,
	}
	err = parser.IsEqualIPNodes(n, ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", n)
	}

	// Test IPv4
	rcIPv4, err := loader.FindFile("GeoLite2-City-Blocks-IPv4.csv", reader)
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create io.ReaderCloser")
	}
	defer rcIPv4.Close()
	ipv4, err := parser.CreateIPList(rcIPv4, idMap, "GeoLite2-City-Blocks-IPv4.csv")
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create ipv4")
	}

	ip, err = search.SearchList(ipv4, "1.0.120.0")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	n = parser.IPNode{
		net.ParseIP("1.0.120.0"),
		net.ParseIP("1.0.123.255"),
		11622,
		"690-0887",
		35.4722,
		133.0506,
	}
	err = parser.IsEqualIPNodes(n, ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", n)
	}

	ip, err = search.SearchList(ipv4, "80.231.5.200")
	if err != nil {
		log.Println(err)
		t.Errorf("Search failed")
	}
	n = parser.IPNode{
		net.ParseIP("80.231.5.0"),
		net.ParseIP("80.231.5.255"),
		0,
		"",
		0,
		0,
	}
	err = parser.IsEqualIPNodes(n, ip)
	if err != nil {
		log.Println(err)
		t.Errorf("Found ", ip, " wanted", n)
	}

}*/
