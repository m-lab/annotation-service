package loadGCS_test

import (
	"testing"
	"os"
	"reflect"

	"github.com/m-lab/annotation-service/createList"
	"github.com/m-lab/annotation-service/loadGCS"

	"google.golang.org/appengine/aetest"
)

func TestInitilizationTable(t *testing.T) {
		var GCSGeoData []createList.Node
		ctx, done, err := aetest.NewContext() 
		if err != nil{
			t.Fatal(err) 
		}
		defer done() 
		loadGCS.InitializeTable(ctx,"test-annotator-sandbox","annotator-data/GEOIPCountryWhois.csv") 
		r,_ := os.Open("createList/testdata/sample.csv") 
		LocalGeoData,_ := createList.CreateList(r) 
		if !reflect.DeepEqual(GCSGeoData,LocalGeoData){
			t.Errorf("Local list and GCS list are inconsistent.")
		}
}
