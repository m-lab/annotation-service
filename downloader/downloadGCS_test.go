package downloader_test

import (
	"archive/zip"
	"errors"
	"fmt"
	"google.golang.org/appengine/aetest"
	"strconv"
	"strings"
	"testing"

	"github.com/m-lab/annotation-service/downloader"
	"github.com/m-lab/annotation-service/parser"
)

func TestInitilizationTable(t *testing.T) {
	err := testFiles("Maxmind/2017/08/15/20170815T200728Z-GeoLite2-City-CSV.zip", "testdata/GeoIPCountryWhoisSAMPLE.csv")
	if err != nil {
		t.Fatal(err)
		t.Fatal("Failed initializing IPv4 table")
	}
}

func TestBadGCSFile(t *testing.T) {
	err := testFiles("Maxmind/2017/08/15/NONEXISTENT.zip", "testdata/GeoIPCountryWhoisSAMPLE.csv")
	if err == nil {
		t.Fatal(err)
		t.Fatal("Failed to recognize nonexistant file")
	}
}

func testFiles(fileName string, localFile string) error {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		return errors.New("Failed context")
	}
	defer done()
	IPv4GCS, IPv6GCS, LocGCS, err := downloader.InitializeTable(ctx, "downloader-mlab-sandbox", fileName)
	if err != nil {
		return errors.New("Failed initializing table")
	}
	//test with local files
	reader, err := zip.OpenReader("testdata/GeoLiteZIP.zip")
	if err != nil {
		return errors.New("error unzipping local file")
	}
	IPv4LOCAL, IPv6LOCAL, LocLOCAL, err := parser.Unzip(&(reader.Reader))

	err = compareIPLists(IPv4GCS, IPv4LOCAL)
	if err != nil {
		return errors.New("IPv4 lists are unequal")
	}
	err = compareIPLists(IPv6GCS, IPv6LOCAL)
	if err != nil {
		return errors.New("IPv6 lists are unequal")

	}
	err = compareLocLists(LocGCS, LocLOCAL)
	if err != nil {
		return errors.New("local lists are unequal")
	}
	return nil
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
			fmt.Println(element)
			fmt.Println(listComp[index])
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
