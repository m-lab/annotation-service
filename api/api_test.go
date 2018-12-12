package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/m-lab/annotation-service/api"
)

func TestExtractDateFromFilename(t *testing.T) {
	date, err := api.ExtractDateFromFilename("Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz")
	if date.Year() != 2017 || date.Month() != 5 || date.Day() != 8 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %v, %+v.", 20170508, date, err)
	}

	date2, err := api.ExtractDateFromFilename("Maxmind/2017/10/05/20171005T033334Z-GeoLite2-City-CSV.zip")
	if date2.Year() != 2017 || date2.Month() != 10 || date2.Day() != 5 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %v, %+v.", 20171005, date2, err)
	}
}

func TestRequestWrapper(t *testing.T) {
	req := api.RequestV2{RequestType: "foobar"}

	bytes, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	wrapper := api.RequestWrapper{}
	err = json.Unmarshal(bytes, &wrapper)
	if err != nil {
		t.Fatal(err)
	}
	switch wrapper.RequestType {
	case req.RequestType:
		err = json.Unmarshal(bytes, &req)
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("wrong request type:", wrapper.RequestType)
	}

	oldReq := []api.RequestData{{"IP", 4, time.Time{}}}
	bytes, err = json.Marshal(oldReq)
	if err != nil {
		t.Fatal(err)
	}
	err = json.Unmarshal(bytes, &wrapper)
	if err == nil {
		t.Fatal("Should have produced json unmarshal error")
	}
}

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestDoV2Request(t *testing.T) {
	expectedJson := `{"AnnotatorDate":"2018-12-05T00:00:00Z","Annotations":{"147.1.2.3":{"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822},"ASN":{}},"8.8.8.8":{"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822},"ASN":{}}}}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, expectedJson)
	}))
	url := ts.URL

	//url = "https://annotator-dot-mlab-sandbox.appspot.com/batch_annotate"
	req := api.RequestV2{Date: time.Now()}
	req.RequestType = api.RequestV2Tag
	req.RequestInfo = "Test"
	ips := []string{"8.8.8.8", "147.1.2.3"}
	resp, err := api.DoV2Request(context.Background(), url, time.Now(), ips)
	if err != nil {
		t.Fatal(err)
	}

	expectedResponse := api.ResponseV2{}
	err = json.Unmarshal([]byte(expectedJson), &expectedResponse)
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(expectedResponse, *resp); diff != nil {
		t.Error(diff)
	}
}
