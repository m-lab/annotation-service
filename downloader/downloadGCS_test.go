package downloader_test

import (
	"google.golang.org/appengine/aetest"
	"os"
	"reflect"
	"testing"

	"github.com/m-lab/annotation-service/downloader"
	"github.com/m-lab/annotation-service/parser"
)

func TestInitilizationTable(t *testing.T) {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	geoData := downloader.InitializeTable(ctx, "test-annotator-sandbox", "annotator-data/GeoIPCountryWhoisSAMPLE.csv")
	r, _ := os.Open("testdata/sample.csv")
	LocalGeoData, _ := parser.CreateList(r)
	if !reflect.DeepEqual(*geoData, LocalGeoData) {
		t.Errorf("Local list and GCS list are inconsistent.")
	}
}
