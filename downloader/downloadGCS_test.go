package downloader_test

import (
	"google.golang.org/appengine/aetest"
	"os"
	"testing"

	"github.com/m-lab/annotation-service/downloader"
	"github.com/m-lab/annotation-service/parser"
)

func TestInitilizationTableIPv4(t *testing.T) {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		t.Fatal("bad context")
	}
	defer done()
	geoData, err := downloader.InitializeTable(ctx, "test-annotator-sandbox", "annotator-data/GeoIPCountryWhoisSAMPLE.csv", 4)
	if err != nil {
		t.Errorf("InitializeTable failed.")
	}
	r, _ := os.Open("testdata/GeoIPCountryWhoisSAMPLE.csv")
	LocalGeoData, _ := parser.CreateList(r,4)
	for index, element := range geoData {
		if !element.LowRangeBin.Equal(LocalGeoData[index].LowRangeBin) {
			t.Errorf("LowRangeBin inconsistent\ngot:%v \nwanted:%v", element.LowRangeBin, LocalGeoData[index].LowRangeBin)
		}
		if !element.HighRangeBin.Equal(LocalGeoData[index].HighRangeBin) {
			t.Errorf("HighRangeBin inconsistent\nngot:%v \nwanted:%v", element.HighRangeBin, LocalGeoData[index].HighRangeBin)

		}
		if element.CountryAbrv != LocalGeoData[index].CountryAbrv {
			t.Errorf("CountryAbrv inconsistent\ngot:%v \nwanted:%v", element.CountryAbrv, LocalGeoData[index].CountryAbrv)

		}
		if element.CountryName != LocalGeoData[index].CountryName {
			t.Errorf("CountryName inconsistent\ngot:%v \nwanted:%v", element.CountryName, LocalGeoData[index].CountryName)
		}
	}
}

func TestInitilizationTableIPv6(t *testing.T) {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	geoData, err := downloader.InitializeTable(ctx, "test-annotator-sandbox", "annotator-data/GeoLiteCityv6SAMPLE.csv", 6)
	if err != nil {
		t.Errorf("Failed initializing table")
	}
	r, _ := os.Open("testdata/GeoLiteCityv6SAMPLE.csv")
	LocalGeoData, _ := parser.CreateList(r,6)
	for index, element := range geoData {
		if !element.LowRangeBin.Equal(LocalGeoData[index].LowRangeBin) {
			t.Errorf("LowRangeBin inconsistent\ngot:%v \nwanted:%v", element.LowRangeBin, LocalGeoData[index].LowRangeBin)
		}
		if !element.HighRangeBin.Equal(LocalGeoData[index].HighRangeBin) {
			t.Errorf("HighRangeBin inconsistent\nngot:%v \nwanted:%v", element.HighRangeBin, LocalGeoData[index].HighRangeBin)

		}
		if element.CountryAbrv != LocalGeoData[index].CountryAbrv {
			t.Errorf("CountryAbrv inconsistent\ngot:%v \nwanted:%v", element.CountryAbrv, LocalGeoData[index].CountryAbrv)

		}
		if element.CountryName != LocalGeoData[index].CountryName {
			t.Errorf("CountryName inconsistent\ngot:%v \nwanted:%v", element.CountryName, LocalGeoData[index].CountryName)
		}
	}
}
