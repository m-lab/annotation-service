package parser_test

import (
	"archive/zip"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/m-lab/annotation-service/parser"
)

func TestUnzip(t *testing.T) {
	//takes source, returns *ReadCloser
	reader, err := zip.OpenReader("testdata/GeoLite2City.zip")
	if err != nil {
		t.Error(err)
	}
	IPv4Test, IPv6Test, LocListTest, err := parser.Unzip(&(reader.Reader))
	if err != nil {
		t.Errorf("Error unzipping and creating lists")
	}
	var IPv6List = []parser.BlockNode{
		parser.BlockNode{
			"600:8801:9400:5a1:948b:ab15:dde3:61a3/128",
			5363990,
			"91941",
			32.7596,
			-116.994,
		},
		parser.BlockNode{
			"2001:5::/32",
			6255148,
			"",
			47,
			8,
		},
		parser.BlockNode{
			"2001:200::/40",
			1861060,
			"",
			36,
			138,
		},
	}
	var IPv4List = []parser.BlockNode{
		parser.BlockNode{
			"1.0.0.0/24",
			2151718,
			"3095",
			-37.7,
			145.1833,
		},
		parser.BlockNode{
			"1.0.1.0/24",
			1810821,
			"",
			26.0614,
			119.3061,
		},
		parser.BlockNode{
			"1.0.2.0/23",
			1810821,
			"",
			26.0614,
			119.3061,
		},
	}
	var LocList = []parser.LocNode{
		parser.LocNode{
			32909,
			"AS",
			"Iran",
			0,
			"Shahre Jadide Andisheh",
		},
		parser.LocNode{
			49518,
			"AF",
			"Rwanda",
			0,
			"",
		},
		parser.LocNode{
			51537,
			"AF",
			"Somalia",
			0,
			"",
		},
	}
	err = compareIPLists(IPv4Test, IPv4List)
	if err != nil {
		t.Errorf("IPv4 lists are not equal")
	}
	err = compareIPLists(IPv6Test, IPv6List)
	if err != nil {
		t.Errorf("IPv6 lists are not equal")
	}
	err = compareLocLists(LocListTest, LocList)
	if err != nil {
		t.Errorf("Location lists are not equal")
	}
}

func compareIPLists(list, listComp []parser.BlockNode) error {
	for index, element := range list {
		if element.IPAddress != listComp[index].IPAddress {
			output := strings.Join([]string{"IPAddress inconsistent\ngot:", element.IPAddress, " \nwanted:", listComp[index].IPAddress}, "")
			fmt.Println(output)
			return errors.New(output)
		}
		if element.Geoname != listComp[index].Geoname {
			output := strings.Join([]string{"Geoname inconsistent\ngot:", strconv.Itoa(element.Geoname), " \nwanted:", strconv.Itoa(listComp[index].Geoname)}, "")
			fmt.Println(output)
			return errors.New(output)
		}
		if element.PostalCode != listComp[index].PostalCode {
			output := strings.Join([]string{"PostalCode inconsistent\ngot:", element.PostalCode, " \nwanted:", listComp[index].PostalCode}, "")
			fmt.Println(output)
			return errors.New(output)
		}
		if element.Latitude != listComp[index].Latitude {
			output := strings.Join([]string{"Latitude inconsistent\ngot:", strconv.FormatFloat(element.Latitude, 'f', 6, 64), " \nwanted:", strconv.FormatFloat(listComp[index].Latitude, 'f', 6, 64)}, "")
			fmt.Println(output)
			return errors.New(output)
		}
		if element.Longitude != listComp[index].Longitude {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", strconv.FormatFloat(element.Longitude, 'f', 6, 64), " \nwanted:", strconv.FormatFloat(listComp[index].Longitude, 'f', 6, 64)}, "")
			fmt.Println(output)
			return errors.New(output)
		}

	}
	return nil
}
func compareLocLists(list, listComp []parser.LocNode) error {
	for index, element := range list {
		if element.Geoname != listComp[index].Geoname {
			output := strings.Join([]string{"Geoname inconsistent\ngot:", strconv.Itoa(element.Geoname), " \nwanted:", strconv.Itoa(listComp[index].Geoname)}, "")
			fmt.Println(output)
			return errors.New(output)
		}
		if element.ContinentCode != listComp[index].ContinentCode {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", element.ContinentCode, " \nwanted:", listComp[index].ContinentCode}, "")
			fmt.Println(output)
			return errors.New(output)
		}
		if element.CountryName != listComp[index].CountryName {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", element.CountryName, " \nwanted:", listComp[index].CountryName}, "")
			fmt.Println(output)
			return errors.New(output)
		}
		if element.MetroCode != listComp[index].MetroCode {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", strconv.FormatInt(element.MetroCode, 16), " \nwanted:", strconv.FormatInt(listComp[index].MetroCode, 16)}, "")
			fmt.Println(output)
			return errors.New(output)
		}
		if element.CityName != listComp[index].CityName {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", element.CityName, " \nwanted:", listComp[index].CityName}, "")
			fmt.Println(output)
			return errors.New(output)
		}
	}
	return nil
}
