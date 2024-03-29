package geoloader_test

import (
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
	if testing.Short() {
		t.Skip("Skipping test that depends on GCS")
	}
	// The downloader-mlab-testing bucket has a snapshot of the datasets
	// as of Sept 22, 2018.  If we ever update it, the numbers here may
	// need to be adjusted.

	v4loader := geoloader.LegacyV4Loader(fakeLoader)
	err := v4loader.UpdateCache()
	if err != nil {
		t.Fatal(err)
	}
	v4 := v4loader.Fetch()
	if len(v4) != 50 {
		t.Error(len(v4))
	}

	v6loader := geoloader.LegacyV6Loader(fakeLoader)
	err = v6loader.UpdateCache()
	if err != nil {
		t.Error(err)
	}
	v6 := v6loader.Fetch()
	if len(v6) != 50 {
		t.Error(len(v6))
	}

	g2loader := geoloader.Geolite2Loader(fakeLoader)
	err = g2loader.UpdateCache()
	if err != nil {
		t.Error(err)
	}
	g2 := g2loader.Fetch()
	if len(g2) != 76 {
		t.Error(len(g2))
	}
}
