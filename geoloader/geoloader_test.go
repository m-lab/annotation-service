package geoloader_test

import (
	"log"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geoloader"
)

type fakeAnn struct {
	startDate time.Time
}

func (f *fakeAnn) Annotate(ip string, ann *api.GeoData) error {
	return nil
}

func (f *fakeAnn) AnnotatorDate() time.Time {
	return f.startDate
}

func (f *fakeAnn) Close() {}

func newFake(date string) *fakeAnn {
	d, _ := time.Parse("20060102", date)
	return &fakeAnn{startDate: d}
}

func fakeLoader(obj *storage.ObjectAttrs) (api.Annotator, error) {
	date, err := api.ExtractDateFromFilename(obj.Name)
	if err != nil {
		return nil, err
	}
	time.Sleep(100 * time.Millisecond)
	return newFake(date.Format("20060102")), nil
}

func TestLoad(t *testing.T) {
	v4, err := geoloader.LoadAllLegacyV4(fakeLoader)
	if err != nil {
		t.Fatal(err)
	}
	if len(v4) != 50 {
		t.Error(len(v4))
	}
	v6, err := geoloader.LoadAllLegacyV6(fakeLoader)
	if err != nil {
		t.Error(err)
	}
	if len(v6) != 50 {
		t.Error(len(v6))
	}

	g2, err := geoloader.LoadAllGeolite2(fakeLoader)
	if err != nil {
		t.Error(err)
	}
	// As more datasets are downloaded, the number of datasets here will increase.
	// As of Feb 2019, there are currently 36.
	if len(g2) < 36 {
		t.Error(len(g2))
	}
}

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
