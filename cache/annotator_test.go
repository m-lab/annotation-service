package cache_test

import (
	"log"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/cache"
	"github.com/m-lab/annotation-service/common"
	"github.com/m-lab/annotation-service/parser"
)

// TODO(JM) Update the test code/data here once we are no longer
// returning a canned response
func TestAnnotate(t *testing.T) {
	tests := []struct {
		req *common.RequestData
		res *common.GeoData
	}{
		{
			req: &common.RequestData{"127.0.0.1", 4, time.Now()},
			res: &common.GeoData{
				Geo: &common.GeolocationIP{City: "Not A Real City", Postal_code: "10583"},
				ASN: &common.IPASNData{}},
		},
	}
	cache.SetLatestDataset(&parser.GeoDataset{
		IP4Nodes: []parser.IPNode{
			{
				IPAddressLow:  net.IPv4(0, 0, 0, 0),
				IPAddressHigh: net.IPv4(255, 255, 255, 255),
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		IP6Nodes: []parser.IPNode{
			{
				IPAddressLow:  net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				IPAddressHigh: net.IP{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		LocationNodes: []parser.LocationNode{
			{
				CityName: "Not A Real City",
			},
		},
	})
	for _, test := range tests {
		res, err := cache.Annotate(test.req)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %v, got %v", test.res, res)
		}
	}
}

func TestSelectArchivedDataset(t *testing.T) {
	testBucket := "downloader-mlab-testing"
	err := cache.Init()
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
	filename, err := cache.SelectArchivedDataset(date1, testBucket, true)
	if filename != "Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz", filename, err)
	}

	date2, _ := time.Parse("January 2, 2006", "March 7, 2014")
	filename2, err := cache.SelectArchivedDataset(date2, testBucket, true)
	if filename2 != "Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz", filename2, err)
	}

	// before the cutoff date.
	date3, _ := time.Parse("January 2, 2006", "August 14, 2017")
	filename3, err := cache.SelectArchivedDataset(date3, testBucket, true)
	if filename3 != "Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz", filename3, err)
	}

	// after the cutoff date.
	date4, _ := time.Parse("January 2, 2006", "August 15, 2017")
	filename4, err := cache.SelectArchivedDataset(date4, testBucket, true)
	if filename4 != "Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip", filename4, err)
	}

	// return the latest available dataset.
	date5, _ := time.Parse("January 2, 2006", "August 15, 2037")
	filename5, err := cache.SelectArchivedDataset(date5, testBucket, true)
	if filename5 != "Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip", filename5, err)
	}

	// before the cutoff date, IPv6
	date6, _ := time.Parse("January 2, 2006", "April 4, 2016")
	filename6, err := cache.SelectArchivedDataset(date6, testBucket, false)
	if filename6 != "Maxmind/2016/03/08/20160308T080000Z-GeoLiteCityv6.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2016/03/08/20160308T080000Z-GeoLiteCityv6.dat.gz", filename6, err)
	}
}

func TestExtractDateFromFilename(t *testing.T) {
	date, err := cache.ExtractDateFromFilename("Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz")
	if date.Year() != 2017 || date.Month() != 5 || date.Day() != 8 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %v, %+v.", 20170508, date, err)
	}

	date2, err := cache.ExtractDateFromFilename("Maxmind/2017/10/05/20171005T033334Z-GeoLite2-City-CSV.zip")
	if date2.Year() != 2017 || date2.Month() != 10 || date2.Day() != 5 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %v, %+v.", 20171005, date2, err)
	}
}
