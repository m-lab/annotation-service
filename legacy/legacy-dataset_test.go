package legacy_test

import (
	"log"
	"testing"

	"github.com/go-test/deep"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/legacy"
)

func TestLoadBundleLegacyDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test that accesses GCS")
	}
	// Note this is slow - 3 to 5 seconds.
	gi, err := legacy.LoadLegacyDataset("Maxmind/2017/04/08/20170408T080000Z-GeoLiteCityv6.dat.gz", "downloader-mlab-testing")
	if err != nil {
		t.Fatal(err)
	}

	ip := "2620:0:1003:415:fa1e:73f3:ec68:7709"
	record := api.GeoData{}
	err = gi.Annotate(ip, &record)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("%v\n", record)
	if diff := deep.Equal(*record.Geo,
		api.GeolocationIP{
			ContinentCode: "NA",
			CountryCode:   "US",
			CountryCode3:  "USA",
			CountryName:   "United States",
			Region:        "",
			MetroCode:     0,
			City:          "",
			AreaCode:      0,
			PostalCode:    "",
			Latitude:      37.751,
			Longitude:     -97.822,
		}); diff != nil {
		t.Error(diff)
	}
}

func TestLoadLegacyGeoliteDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test that accesses GCS")
	}
	gi, err := legacy.LoadGeoliteDataset("Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz", "downloader-mlab-testing")
	if err != nil {
		log.Printf("Did not load legacy dataset correctly %v", err)
	}
	if gi != nil {
		record := gi.GetRecord("207.171.7.51", true)
		if record == nil {
			t.Fatal("record is nil")
		}
		expected := legacy.GeoIPRecord{
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
		}
		if diff := deep.Equal(*record, expected); diff != nil {
			t.Error(diff)
		}
	}
}

func TestLoadLegacyGeoliteV6Dataset(t *testing.T) {
	gi, err := legacy.LoadGeoliteDataset("Maxmind/2014/03/07/20140307T160000Z-GeoLiteCityv6.dat.gz", "downloader-mlab-testing")
	if err != nil {
		log.Printf("Did not load legacy dataset correctly %v", err)
	}
	if gi != nil {
		record := gi.GetRecord("2620:0:1003:415:fa1e:73f3:ec68:7709", false)
		expected := legacy.GeoIPRecord{
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
		}

		if record == nil {
			t.Fatal("record is nil")
		}
		if diff := deep.Equal(*record, expected); diff != nil {
			t.Error(diff)
		}
	}
}
