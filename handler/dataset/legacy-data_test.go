package dataset_test

import (
	"fmt"
	"testing"

	"github.com/m-lab/annotation-service/handler/dataset"
	"github.com/m-lab/annotation-service/handler/geoip"
	. "gopkg.in/check.v1"
)

func TestExtractDateFromFilename(t *testing.T) {
	date, err := dataset.ExtractDateFromFilename("Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz")
	if date != 20170508 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %d, %+v.", 20170508, date, err)
	}

	date2, err := dataset.ExtractDateFromFilename("Maxmind/2017/10/05/20171005T033334Z-GeoLite2-City-CSV.zip")
	if date2 != 20171005 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %d, %+v.", 20171005, date2, err)
	}
}

func TestSelectGeoLegacyFile(t *testing.T) {
	filename, err := dataset.SelectGeoLegacyFile(20110203, "downloader-mlab-testing")
	if filename != "Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz", filename, err)
	}

	filename2, err := dataset.SelectGeoLegacyFile(20140307, "downloader-mlab-testing")
	if filename2 != "Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz", filename2, err)
	}

	// before the cutoff date.
	filename3, err := dataset.SelectGeoLegacyFile(20170814, "downloader-mlab-testing")
	if filename3 != "Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz", filename3, err)
	}

	// after the cutoff date.
	filename4, err := dataset.SelectGeoLegacyFile(20170815, "downloader-mlab-testing")
	if filename4 != "Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip", filename4, err)
	}

	// return the latest available dataset.
	filename5, err := dataset.SelectGeoLegacyFile(20370820, "downloader-mlab-testing")
	if filename5 != "Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip", filename5, err)
	}
}

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type GeoIPSuite struct {
}

var _ = Suite(&GeoIPSuite{})

func (s *GeoIPSuite) TestLoadLegacyGeoliteDataset(c *C) {
	gi, err := dataset.LoadLegacyGeoliteDataset(20140203, "downloader-mlab-testing")
	fmt.Printf("%v", err)
	if gi != nil {
		record := gi.GetRecord("207.171.7.51")
		c.Assert(record, NotNil)
		c.Check(
			*record,
			Equals,
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
