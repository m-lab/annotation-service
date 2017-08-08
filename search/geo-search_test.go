package search_test

import (
	"errors"
	"google.golang.org/appengine/aetest"
	"log"
	"net"
	"strings"
	"testing"

	"github.com/m-lab/annotation-service/downloader"
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/search"
)

var listComp = []parser.Node{
	parser.Node{
		LowRangeBin:  net.IPv4(1, 0, 1, 0),
		HighRangeBin: net.IPv4(1, 0, 3, 255),
		CountryAbrv:  "CN",
		CountryName:  "China",
	},
	parser.Node{
		LowRangeBin:  net.IPv4(1, 0, 4, 0),
		HighRangeBin: net.IPv4(1, 0, 7, 255),
		CountryAbrv:  "AU",
		CountryName:  "Australia",
	},
	parser.Node{
		LowRangeBin:  net.IPv4(1, 0, 8, 0),
		HighRangeBin: net.IPv4(1, 0, 15, 255),
		CountryAbrv:  "CN",
		CountryName:  "China",
	},
	parser.Node{
		LowRangeBin:  net.IPv4(1, 0, 16, 0),
		HighRangeBin: net.IPv4(1, 0, 31, 255),
		CountryAbrv:  "JP",
		CountryName:  "Japan",
	},
}

func TestLocalSearchList(t *testing.T) {
	if checkSearch("1.0.4.0", "AU", listComp) != nil {
		t.Errorf("Search #1 FAILED")
	}
	if checkSearch("1.0.4.1", "AU", listComp) != nil {
		t.Errorf("Search #3 FAILED")
	}
	if checkSearch("1.0.30.1", "JP", listComp) != nil {
		t.Errorf("Search #4 FAILED")
	}

	//IP not found in list
	if checkSearch("3.0.4.0", "AU", listComp) == nil {
		t.Errorf("Search #5 FAILED")
	}
	// invalid search IP
	if checkSearch("ABCDEFGHS", "AU", listComp) == nil {
		t.Errorf("Search #6 FAILED")
	}

}

func TestGCSSearchListIPv4(t *testing.T) {
	if searchGCS("1.32.0.1", "MY", "annotator-data/GeoIPCountryWhois.csv", 4) != nil {
		t.Errorf("IPv4 #1 Failed")
	}
	if searchGCS("5.11.56.4", "GR", "annotator-data/GeoIPCountryWhois.csv", 4) != nil {
		t.Errorf("IPv4 #2 Failed")
	}
	if searchGCS("5.44.239.254", "GB", "annotator-data/GeoIPCountryWhois.csv", 4) != nil {
		t.Errorf("IPv4 #3 Failed")
	}

	//incorrect input
	if searchGCS("5.54.4.32", "GB", "annotator-data/GeoIPCountryWhois.csv", 4) == nil {
		t.Errorf("IPv4 #4 Failed")
	}

}

func TestGCSSearchListIPv6(t *testing.T) {
	if searchGCS("2001:500:2::", "US", "annotator-data/GeoLiteCityv6.csv", 6) != nil {
		t.Errorf("IPv6 #1 Failed")
	}
	if searchGCS("2001:678:a:ffff:ffff:ffff:ffff:ffff", "BE", "annotator-data/GeoLiteCityv6.csv", 6) != nil {
		t.Errorf("IPv6 #2 Failed")
	}
	if searchGCS("2001:678:2f4::", "IL", "annotator-data/GeoLiteCityv6.csv", 6) != nil {
		t.Errorf("IPv6 #3 Failed")
	}

	//incorrect input 
	if searchGCS("2001:678:45c::", "GB", "annotator-data/GeoLiteCityv6.csv", 6) == nil {
		t.Errorf("IPv6 #4 Failed")
	}
}

func searchGCS(IPLookUp, abrv, fileName string, IPversion int) error {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		log.Println("Failed context")
		return errors.New("Failed context")
	}
	defer done()
	geoData, err := downloader.InitializeTable(ctx, "test-annotator-sandbox", fileName, IPversion)
	if err != nil {
		log.Println("Failed initializing table")
		return errors.New("Failed initializing table")
	}
	if checkSearch(IPLookUp, abrv, geoData) != nil {
		log.Println("checkSearch Failed.")
		return errors.New("checkSearch Failed.")
	}
	return nil

}
func checkSearch(IPLookUp, abrv string, listComp []parser.Node) error {
	n, err := search.SearchList(listComp, IPLookUp)
	if err != nil {
		output := strings.Join([]string{"expecting ", abrv, " got: Node not found"}, "")
		log.Println(output)
		return errors.New(output)
	} else if n.CountryAbrv != abrv {
		output := strings.Join([]string{"expecting ", abrv, " got: ", n.CountryAbrv}, "")
		log.Println(output)
		return errors.New(output)
	}
	return nil
}
