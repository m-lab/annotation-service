package downloader_test

import (
	"testing"
	"os"
	"reflect"

	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/downloader"

	"google.golang.org/appengine/aetest"
)

func TestInitilizationTable(t *testing.T) {
		ctx, done, err := aetest.NewContext() 
		if err != nil{
			t.Fatal(err) 
		}
		defer done()
		geoData := downloader.InitializeTable(ctx,"test-annotator-sandbox","annotator-data/GeoIPCountryWhoisSAMPLE.csv") 
		r,_ := os.Open("testdata/sample.csv") 
		LocalGeoData,_ := parser.CreateList(r) 
		if !reflect.DeepEqual(*geoData,LocalGeoData){
			t.Errorf("Local list and GCS list are inconsistent.")
		}
}
