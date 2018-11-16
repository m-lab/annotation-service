package handler_test

import (
	"log"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/handler/geoip"
	check "gopkg.in/check.v1"
)

func TestSelectGeoLegacyFile(t *testing.T) {
	testBucket := "downloader-mlab-testing"
	err := handler.UpdateFilenamelist(testBucket)
	if err != nil {
		// TODO: make dataset produce rich error types to allow us to
		// distinguish between auth error (which should cause us to
		// skip the rest of the tests) and all other error types (which
		// should properly be errors and cause the test to fail).
		log.Println("cannot load test datasets")
		log.Println("This can happen when running tests from branches outside of github.com/m-lab/annotation-server.  The rest of this test is being skipped.")
		return
	}
	date1, _ := time.Parse("January 2, 2006", "January 3, 2011")
	filename, err := handler.SelectGeoLegacyFile(date1, testBucket, true)
	if filename != "Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz", filename, err)
	}

	date2, _ := time.Parse("January 2, 2006", "March 7, 2014")
	filename2, err := handler.SelectGeoLegacyFile(date2, testBucket, true)
	if filename2 != "Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz", filename2, err)
	}

	// before the cutoff date.
	date3, _ := time.Parse("January 2, 2006", "August 14, 2017")
	filename3, err := handler.SelectGeoLegacyFile(date3, testBucket, true)
	if filename3 != "Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz", filename3, err)
	}

	// after the cutoff date.
	date4, _ := time.Parse("January 2, 2006", "August 15, 2017")
	filename4, err := handler.SelectGeoLegacyFile(date4, testBucket, true)
	if filename4 != "Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip", filename4, err)
	}

	// return the latest available dataset.
	date5, _ := time.Parse("January 2, 2006", "August 15, 2037")
	filename5, err := handler.SelectGeoLegacyFile(date5, testBucket, true)
	if filename5 != "Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip", filename5, err)
	}

	// before the cutoff date, IPv6
	date6, _ := time.Parse("January 2, 2006", "April 4, 2016")
	filename6, err := handler.SelectGeoLegacyFile(date6, testBucket, false)
	if filename6 != "Maxmind/2016/03/08/20160308T080000Z-GeoLiteCityv6.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2016/03/08/20160308T080000Z-GeoLiteCityv6.dat.gz", filename6, err)
	}
}

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { check.TestingT(t) }

type GeoIPSuite struct {
}

var _ = check.Suite(&GeoIPSuite{})

func (s *GeoIPSuite) TestLoadLegacyGeoliteDataset(c *check.C) {
	gi, err := handler.LoadLegacyGeoliteDataset("Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz", "downloader-mlab-testing")
	if err != nil {
		log.Printf("Did not load legacy dataset correctly %v", err)
	}
	if gi != nil {
		record := gi.GetRecord("207.171.7.51", true)
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

func (s *GeoIPSuite) TestLoadLegacyGeoliteV6Dataset(c *check.C) {
	gi, err := handler.LoadLegacyGeoliteDataset("Maxmind/2014/03/07/20140307T160000Z-GeoLiteCityv6.dat.gz", "downloader-mlab-testing")
	if err != nil {
		log.Printf("Did not load legacy dataset correctly %v", err)
	}
	if gi != nil {
		record := gi.GetRecord("2620:0:1003:415:fa1e:73f3:ec68:7709", false)
		c.Assert(record, check.NotNil)
		c.Check(
			*record,
			check.Equals,
			geoip.GeoIPRecord{
				CountryCode:   "US",
				CountryCode3:  "USA",
				CountryName:   "United States",
				Region:        "",
				City:          "",
				PostalCode:    "",
				Latitude:      38,
				Longitude:     -97,
				AreaCode:      00,
				MetroCode:     0,
				CharSet:       1,
				ContinentCode: "NA",
			},
		)
	}
}
