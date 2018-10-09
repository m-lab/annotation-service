package dataset_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/handler/dataset"
	"github.com/m-lab/annotation-service/handler/geoip"
	check "gopkg.in/check.v1"
)

func TestExtractDateFromFilename(t *testing.T) {
	date, err := dataset.ExtractDateFromFilename("Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz")
	if date.Year() != 2017 || date.Month() != 5 || date.Day() != 8 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %v, %+v.", 20170508, date, err)
	}

	date2, err := dataset.ExtractDateFromFilename("Maxmind/2017/10/05/20171005T033334Z-GeoLite2-City-CSV.zip")
	if date2.Year() != 2017 || date2.Month() != 10 || date2.Day() != 5 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %v, %+v.", 20171005, date2, err)
	}
}

func TestSelectGeoLegacyFile(t *testing.T) {
	testBucket := "downloader-mlab-testing"
	err := dataset.UpdateFilenamelist(testBucket)
	if err != nil {
		t.Errorf("cannot load test datasets")
	}
	date1, _ := time.Parse("January 2, 2006", "January 3, 2011")
	filename, err := dataset.SelectGeoLegacyFile(date1, testBucket)
	if filename != "Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz", filename, err)
	}

	date2, _ := time.Parse("January 2, 2006", "March 7, 2014")
	filename2, err := dataset.SelectGeoLegacyFile(date2, testBucket)
	if filename2 != "Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz", filename2, err)
	}

	// before the cutoff date.
	date3, _ := time.Parse("January 2, 2006", "August 14, 2017")
	filename3, err := dataset.SelectGeoLegacyFile(date3, testBucket)
	if filename3 != "Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz", filename3, err)
	}

	// after the cutoff date.
	date4, _ := time.Parse("January 2, 2006", "August 15, 2017")
	filename4, err := dataset.SelectGeoLegacyFile(date4, testBucket)
	if filename4 != "Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip", filename4, err)
	}

	// return the latest available dataset.
	date5, _ := time.Parse("January 2, 2006", "August 15, 2037")
	filename5, err := dataset.SelectGeoLegacyFile(date5, testBucket)
	if filename5 != "Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip", filename5, err)
	}
}

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { check.TestingT(t) }

type GeoIPSuite struct {
}

var _ = check.Suite(&GeoIPSuite{})

func (s *GeoIPSuite) TestLoadLegacyGeoliteDataset(c *check.C) {
	date1, _ := time.Parse("January 2, 2006", "February 3, 2014")
	gi, err := dataset.LoadLegacyGeoliteDataset(date1, "downloader-mlab-testing")
	fmt.Printf("%v", err)
	if gi != nil {
		record := gi.GetRecord("207.171.7.51")
		c.Assert(record, check.NotNil)
		c.Check(
			*record,
			check.Equals,
			geoip.GeoIPRecord{
				CountryCode:   "US",
				CountryCode3:  "USA",
				CountryName:   "United States",
				Region:        "CA",
				City:          "El Segundo",
				PostalCode:    "90245",
				Latitude:      33.9164,
				Longitude:     -118.4041,
				AreaCode:      310,
				MetroCode:     803,
				CharSet:       1,
				ContinentCode: "NA",
			},
		)
	}
}
