package search_test

import (
	"errors"
	"net"
	"strings"
	"testing"

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

func TestSearchList(t *testing.T) {
	if checkSearch("1.0.4.0", listComp[1]) != nil {
		t.Errorf("Search #1 FAILED")
	}
	if checkSearch("3.0.4.0", listComp[0]) == nil {
		t.Errorf("Search #2 FAILED")
	}
	if checkSearch("1.0.4.1", listComp[1]) != nil {
		t.Errorf("Search #3 FAILED")
	}
	if checkSearch("1.0.30.1", listComp[3]) != nil {
		t.Errorf("Search #4 FAILED")
	}

}
func checkSearch(IPLookUp string, ans parser.Node) error {
	n, err := search.SearchList(listComp, IPLookUp)
	if err != nil {
		output := strings.Join([]string{"expecting", ans.CountryName, "got: Node not found"}, "")
		return errors.New(output)
	} else if n.CountryName != ans.CountryName {
		output := strings.Join([]string{"expecting", ans.CountryName, "got: ", n.CountryName}, "")
		return errors.New(output)
	}
	return nil
}
