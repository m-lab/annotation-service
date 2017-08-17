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
	err := testFiles("MaxMind/2017/08/15/GeoLite2City.zip", "testdata/GeoLite2City.zip")
	if err != nil {
		t.Fatal("Failed initializing IPv4 table")
	}
}

func testFiles(fileName string, localFile string) error {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		return errors.New("Failed context")
	}
	defer done()
	IPv4GCS, IPv6GCS, LocationGCS, err := downloader.InitializeTable(ctx, "test-annotator-sandbox", fileName)
	if err != nil {
		return errors.New("Failed initializing table")
	}
	// Test with local files
	reader, err := zip.OpenReader(localFile)
	if err != nil {
		return errors.New("error unzipping local file")
	}
	IPv4LOCAL, IPv6LOCAL, LocationLOCAL, err := parser.Unzip(&(reader.Reader))

	err = compareIPLists(IPv4GCS, IPv4LOCAL)
	if err != nil {
		return errors.New("IPv4 lists are unequal")
	}
	err = compareIPLists(IPv6GCS, IPv6LOCAL)
	if err != nil {
		return errors.New("IPv6 lists are unequal")

	}
	err = compareLocLists(LocationGCS, LocationLOCAL)
	if err != nil {
		return errors.New("local lists are unequal")
	}
	return nil
}
func compareIPLists(list, listComp []parser.BlockNode) error {
	for index, element := range list {
		if element.IPAddress != listComp[index].IPAddress {
			output := strings.Join([]string{"IPAddress inconsistent\ngot:", element.IPAddress, " \nwanted:", listComp[index].IPAddress}, "")
			return errors.New(output)
		}
		if element.Geoname != listComp[index].Geoname {
			output := strings.Join([]string{"Geoname inconsistent\ngot:", strconv.Itoa(element.Geoname), " \nwanted:", strconv.Itoa(listComp[index].Geoname)}, "")
			return errors.New(output)
		}
		if element.PostalCode != listComp[index].PostalCode {
			output := strings.Join([]string{"PostalCode inconsistent\ngot:", element.PostalCode, " \nwanted:", listComp[index].PostalCode}, "")
			return errors.New(output)
		}
		if element.Latitude != listComp[index].Latitude {
			output := strings.Join([]string{"Latitude inconsistent\ngot:", floatFormat(element.Latitude) , " \nwanted:", floatFormat(listComp[index].Latitude)}, "")
			return errors.New(output)
		}
		if element.Longitude != listComp[index].Longitude {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", floatFormat(element.Longitude), " \nwanted:", floatFormat(listComp[index].Longitude)}, "")
			return errors.New(output)
		}

	}
	return nil
}
func floatFormat(f float64) string{
	return strconv.FormatFloat(f, 'f', 6, 64)
}
func compareLocLists(list, listComp []parser.LocationNode) error {
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
