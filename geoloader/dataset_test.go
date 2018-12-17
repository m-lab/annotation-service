package geoloader_test

import (
	"log"
	"testing"

	"github.com/go-test/deep"
	"github.com/m-lab/annotation-service/geoloader"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestParseDataset(t *testing.T) {
	tests := []struct {
		name string
		want geoloader.DatasetInfo
	}{
		{"Maxmind/2018/06/01/20180601T053724Z-GeoLiteCity-latest.zip",
			geoloader.DatasetInfo{"Maxmind/2018/06/01/20180601T053724Z-GeoLiteCity-latest.zip", "Maxmind", "2018/06/01", "20180601T053724Z", "GeoLiteCity", "-latest", "zip", 1, false}},
		{"Maxmind/2018/06/01/20180601T053727Z-GeoLiteCityv6.csv.gz",
			geoloader.DatasetInfo{"Maxmind/2018/06/01/20180601T053727Z-GeoLiteCityv6.csv.gz", "Maxmind", "2018/06/01", "20180601T053727Z", "GeoLiteCity", "", "csv.gz", 1, true}},
		{"Maxmind/2018/06/01/20180601T053730Z-GeoLite2-City-CSV.zip",
			geoloader.DatasetInfo{"Maxmind/2018/06/01/20180601T053730Z-GeoLite2-City-CSV.zip", "Maxmind", "2018/06/01", "20180601T053730Z", "GeoLite2-City-CSV", "", "zip", 2, false}},
		{"Maxmind/2018/06/01/20180601T053732Z-GeoLite2-Country-CSV.zip",
			geoloader.DatasetInfo{"Maxmind/2018/06/01/20180601T053732Z-GeoLite2-Country-CSV.zip", "Maxmind", "2018/06/01", "20180601T053732Z", "GeoLite2-Country-CSV", "", "zip", 2, false}},
		{"Maxmind/2018/06/01/20180601T053733Z-GeoLite2-ASN-CSV.zip",
			geoloader.DatasetInfo{"Maxmind/2018/06/01/20180601T053733Z-GeoLite2-ASN-CSV.zip", "Maxmind", "2018/06/01", "20180601T053733Z", "GeoLite2-ASN-CSV", "", "zip", 2, false}},
		{"Maxmind/2018/06/01/20180601T053734Z-GeoLite2-City.tar.gz",
			geoloader.DatasetInfo{"Maxmind/2018/06/01/20180601T053734Z-GeoLite2-City.tar.gz", "Maxmind", "2018/06/01", "20180601T053734Z", "GeoLite2-City", "", "tar.gz", 2, false}},
		{"Maxmind/2018/06/01/20180601T053736Z-GeoLite2-ASN.tar.gz",
			geoloader.DatasetInfo{"Maxmind/2018/06/01/20180601T053736Z-GeoLite2-ASN.tar.gz", "Maxmind", "2018/06/01", "20180601T053736Z", "GeoLite2-ASN", "", "tar.gz", 2, false}},
		{"Maxmind/2018/06/01/20180601T053736Z-GeoLite2-Country.tar.gz",
			geoloader.DatasetInfo{"Maxmind/2018/06/01/20180601T053736Z-GeoLite2-Country.tar.gz", "Maxmind", "2018/06/01", "20180601T053736Z", "GeoLite2-Country", "", "tar.gz", 2, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := geoloader.ParseDataset(tt.name)
			if diff := deep.Equal(got, tt.want); diff != nil {
				log.Println(tt.name)
				t.Error(diff)
			}
		})
	}
}
