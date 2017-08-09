package downloader_test

import (
	"errors"
	"google.golang.org/appengine/aetest"
	"os"
	"strings"
	"testing"

	"github.com/m-lab/annotation-service/downloader"
	"github.com/m-lab/annotation-service/parser"
)


func TestInitilizationTableIPv4(t *testing.T) {
	if testFiles("annotator-data/MaxMind/GeoIPCountryWhoisSAMPLE.csv", 4, "testdata/GeoIPCountryWhoisSAMPLE.csv") != nil {
		t.Fatal("Failed initializing IPv4 table")
	}
}
func TestInitilizationTableIPv6(t *testing.T) {
	if testFiles("annotator-data/MaxMind/GeoLiteCityv6SAMPLE.csv", 6, "testdata/GeoLiteCityv6SAMPLE.csv") != nil {
		t.Fatal("Failed initilaizing IPv4 table")
	}
}

func testFiles(fileName string, IPversion int, localFile string) error {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		return errors.New("Failed context")
	}
	defer done()
	geoData, err := downloader.InitializeTable(ctx, "test-annotator-sandbox", fileName, IPversion)
	if err != nil {
		return errors.New("Failed initializing table")
	}
	r, err := os.Open(localFile)
	if err != nil {
		return errors.New("Invalid file")
	}
	LocalGeoData, err := parser.CreateList(r, IPversion)
	if err != nil {
		return errors.New("List could not be created")
	}
	for index, element := range geoData {
		if !element.LowRangeBin.Equal(LocalGeoData[index].LowRangeBin) {
			output := strings.Join([]string{"LowRangeBin inconsistent\ngot:", element.LowRangeBin.String(), " \nwanted:", LocalGeoData[index].LowRangeBin.String()}, "")
			return errors.New(output)
		}
		if !element.HighRangeBin.Equal(LocalGeoData[index].HighRangeBin) {
			output := strings.Join([]string{"HighRangeBin inconsistent\ngot:", element.HighRangeBin.String(), " \nwanted:", LocalGeoData[index].HighRangeBin.String()}, "")
			return errors.New(output)
		}
		if element.CountryAbrv != LocalGeoData[index].CountryAbrv {
			output := strings.Join([]string{"CountryAbrv inconsistent\ngot:", element.CountryAbrv, " \nwanted:", LocalGeoData[index].CountryAbrv}, "")
			return errors.New(output)
		}
		if element.CountryName != LocalGeoData[index].CountryName {
			output := strings.Join([]string{"CountryName inconsistent\ngot:", element.CountryName, " \nwanted:", LocalGeoData[index].CountryName}, "")
			return errors.New(output)
		}
	}
	return nil
}
