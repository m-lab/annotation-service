package parser_test

import (
	"github.com/m-lab/annotation-service/parser"
	"net"
	"os"
	"testing"
)

//tests correct parsing of createList
func TestCreateListIPv4(t *testing.T) {
	r, _ := os.Open("testdata/sample.csv")
	list, _ := parser.CreateListIPv4(r)
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
	for index, element := range list {
		if !element.LowRangeBin.Equal(listComp[index].LowRangeBin) {
			t.Errorf("LowRangeBin inconsistent\ngot:%v \nwanted:%v", element.LowRangeBin, listComp[index].LowRangeBin)
		}
		if !element.HighRangeBin.Equal(listComp[index].HighRangeBin) {
			t.Errorf("HighRangeBin inconsistent\nngot:%v \nwanted:%v", element.HighRangeBin, listComp[index].HighRangeBin)

		}
		if element.CountryAbrv != listComp[index].CountryAbrv {
			t.Errorf("CountryAbrv inconsistent\ngot:%v \nwanted:%v", element.CountryAbrv, listComp[index].CountryAbrv)

		}
		if element.CountryName != listComp[index].CountryName {
			t.Errorf("CountryName inconsistent\ngot:%v \nwanted:%v", element.CountryName, listComp[index].CountryName)
		}
	}
}
func TestCreateListIPv6(t *testing.T) {
	r, _ := os.Open("testdata/IPv6SAMPLE.csv")
	list, _ := parser.CreateListIPv6(r)
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
	for index, element := range list {
		if !element.LowRangeBin.Equal(listComp[index].LowRangeBin) {
			t.Errorf("LowRangeBin inconsistent\ngot:%v \nwanted:%v", element.LowRangeBin, listComp[index].LowRangeBin)
		}
		if !element.HighRangeBin.Equal(listComp[index].HighRangeBin) {
			t.Errorf("HighRangeBin inconsistent\nngot:%v \nwanted:%v", element.HighRangeBin, listComp[index].HighRangeBin)

		}
		if element.CountryAbrv != listComp[index].CountryAbrv {
			t.Errorf("CountryAbrv inconsistent\ngot:%v \nwanted:%v", element.CountryAbrv, listComp[index].CountryAbrv)

		}
		if element.CountryName != listComp[index].CountryName {
			t.Errorf("CountryName inconsistent\ngot:%v \nwanted:%v", element.CountryName, listComp[index].CountryName)
		}
	}

}
