package legacy_test

import (
	"log"

	"github.com/m-lab/annotation-service/legacy"
	check "gopkg.in/check.v1"
)

var _ = check.Suite(&GeoIPSuite{})

func (s *GeoIPSuite) TestLoadLegacyGeoliteDataset(c *check.C) {
	gi, err := legacy.LoadLegacyGeoliteDataset("Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz", "downloader-mlab-testing")
	if err != nil {
		log.Printf("Did not load legacy dataset correctly %v", err)
	}
	if gi != nil {
		record := gi.GetRecord("207.171.7.51", true)
		c.Assert(record, check.NotNil)
		c.Check(
			*record,
			check.Equals,
			legacy.GeoIPRecord{
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
	gi, err := legacy.LoadLegacyGeoliteDataset("Maxmind/2014/03/07/20140307T160000Z-GeoLiteCityv6.dat.gz", "downloader-mlab-testing")
	if err != nil {
		log.Printf("Did not load legacy dataset correctly %v", err)
	}
	if gi != nil {
		record := gi.GetRecord("2620:0:1003:415:fa1e:73f3:ec68:7709", false)
		c.Assert(record, check.NotNil)
		c.Check(
			*record,
			check.Equals,
			legacy.GeoIPRecord{
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
