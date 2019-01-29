package geoloader_test

import (
	"log"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geoloader"
)

func TestBestAnnotatorFilename(t *testing.T) {
	// TODO use a new dataset instead of the var.
	err := geoloader.UpdateArchivedFilenames()
	if err != nil {
		// TODO: make dataset produce rich error types to allow us to
		// distinguish between auth error (which should cause us to
		// skip the rest of the tests) and all other error types (which
		// should properly be errors and cause the test to fail).
		log.Println("cannot load test datasets")
		log.Println("This can happen when running tests from branches outside of github.com/m-lab/annotation-server.  The rest of this test is being skipped.")
		return
	}
	// Should return the earliest available dataset.
	date1, _ := time.Parse("January 2, 2006", "January 3, 2011")
	filename := geoloader.BestAnnotatorFilename(&api.RequestData{IP: "8.8.8.8", IPFormat: 4, Timestamp: date1})
	if filename != "Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz", filename, err)
	}

	date2, _ := time.Parse("January 2, 2006", "March 8, 2014")
	filename2 := geoloader.BestAnnotatorFilename(&api.RequestData{IP: "8.8.8.8", IPFormat: 4, Timestamp: date2})
	if filename2 != "Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz", filename2, err)
	}

	// before the cutoff date.
	date3, _ := time.Parse("January 2, 2006", "August 15, 2017")
	filename3 := geoloader.BestAnnotatorFilename(&api.RequestData{IP: "8.8.8.8", IPFormat: 4, Timestamp: date3})
	if filename3 != "Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz", filename3, err)
	}

	// after the cutoff date.
	date4, _ := time.Parse("January 2, 2006", "August 16, 2017")
	filename4 := geoloader.BestAnnotatorFilename(&api.RequestData{IP: "8.8.8.8", IPFormat: 4, Timestamp: date4})
	if filename4 != "Maxmind/2017/08/15/20170815T200728Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/15/20170815T200728Z-GeoLite2-City-CSV.zip", filename4, err)
	}

	// return the latest available dataset.
	date5, _ := time.Parse("January 2, 2006", "August 15, 2037")
	filename5 := geoloader.BestAnnotatorFilename(&api.RequestData{IP: "8.8.8.8", IPFormat: 4, Timestamp: date5})
	if filename5 != "Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip", filename5, err)
	}

	// before the cutoff date, IPv6
	date6, _ := time.Parse("January 2, 2006", "April 4, 2016")
	filename6 := geoloader.BestAnnotatorFilename(&api.RequestData{IP: "FF::FF", IPFormat: 6, Timestamp: date6})
	if filename6 != "Maxmind/2016/03/08/20160308T080000Z-GeoLiteCityv6.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2016/03/08/20160308T080000Z-GeoLiteCityv6.dat.gz", filename6, err)
	}
}
