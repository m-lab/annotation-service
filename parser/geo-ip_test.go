package parser_test

import (
	"errors"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/m-lab/annotation-service/parser"
)

//tests correct parsing of createList
func TestCreateListIPv4(t *testing.T) {
	r, err := os.Open("testdata/IPv4SAMPLE.csv")
	if err != nil {
		t.Errorf("Error opening IPv4SAMPLE.csv")
	}
	list, err := parser.CreateList(r, 4)
	if err != nil {
		t.Errorf("Error in creating list")
	}
	var listComp = []parser.Node{
		parser.Node{
			net.IP{1, 0, 1, 0},
			net.IP{1, 0, 3, 255},
			"CN",
			"China",
		},
		parser.Node{
			net.IP{1, 0, 4, 0},
			net.IP{1, 0, 7, 255},
			"AU",
			"Australia",
		},
		parser.Node{
			net.IP{1, 0, 8, 0},
			net.IP{1, 0, 15, 255},
			"CN",
			"China",
		},
		parser.Node{
			net.IP{1, 0, 16, 0},
			net.IP{1, 0, 31, 255},
			"JP",
			"Japan",
		},
	}
	if compareLists(list, listComp) != nil {
		t.Errorf("CreateList failed.")
	}

}
func TestCreateListIPv6(t *testing.T) {
	r, err := os.Open("testdata/IPv6SAMPLE.csv")
	if err != nil {
		t.Errorf("Error opening IPv6SAMPLE.csv")
	}
	list, err := parser.CreateList(r, 6)
	if err != nil {
		t.Errorf("Error in creating list")
	}
	var listComp = []parser.Node{
		parser.Node{
			net.ParseIP("2001:5::"),
			net.ParseIP("2001:5:ffff:ffff:ffff:ffff:ffff:ffff"),
			"EU",
			"N/A",
		},
		parser.Node{
			net.ParseIP("2001:200::"),
			net.ParseIP("2001:200:ffff:ffff:ffff:ffff:ffff:ffff"),
			"JP",
			"N/A",
		},
		parser.Node{
			net.ParseIP("2001:208::"),
			net.ParseIP("2001:208:ffff:ffff:ffff:ffff:ffff:ffff"),
			"SG",
			"N/A",
		},
		parser.Node{
			net.ParseIP("2001:218::"),
			net.ParseIP("2001:218:ffff:ffff:ffff:ffff:ffff:ffff"),
			"JP",
			"N/A",
		},
	}
	if compareLists(list, listComp) != nil {
		t.Errorf("CreateList failed.")
	}

}

func TestCorruptedCode(t *testing.T) {
	r, _ := os.Open("testdata/IPv4CORRUPT.csv")
	_, err := parser.CreateList(r, 4)
	if err == nil {
		t.Errorf("did not catch corrupted data")
	}
}

func compareLists(list, listComp []parser.Node) error {
	for index, element := range list {
		if !element.LowRangeBin.Equal(listComp[index].LowRangeBin) {
			output := strings.Join([]string{"LowRangeBin inconsistent\ngot:", element.LowRangeBin.String(), " \nwanted:", listComp[index].LowRangeBin.String()}, "")
			return errors.New(output)
		}
		if !element.HighRangeBin.Equal(listComp[index].HighRangeBin) {
			output := strings.Join([]string{"HighRangeBin inconsistent\ngot:", element.HighRangeBin.String(), " \nwanted:", listComp[index].HighRangeBin.String()}, "")
			return errors.New(output)
		}
		if element.CountryAbrv != listComp[index].CountryAbrv {
			output := strings.Join([]string{"CountryAbrv inconsistent\ngot:", element.CountryAbrv, " \nwanted:", listComp[index].CountryAbrv}, "")
			return errors.New(output)
		}
		if element.CountryName != listComp[index].CountryName {
			output := strings.Join([]string{"CountryName inconsistent\ngot:", element.CountryName, " \nwanted:", listComp[index].CountryName}, "")
			return errors.New(output)
		}

	}
	return nil
}
