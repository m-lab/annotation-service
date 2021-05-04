package api_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	types "github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/annotation-service/site"
	"github.com/m-lab/go/content"
	"github.com/m-lab/go/rtx"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

var (
	localRawfile content.Provider
	retiredFile  content.Provider
)

func setUp() {
	u, err := url.Parse("file:testdata/annotations.json")
	rtx.Must(err, "Could not parse URL")
	localRawfile, err = content.FromURL(context.Background(), u)
	rtx.Must(err, "Could not create content.Provider")

	u, err = url.Parse("file:testdata/retired-annotations.json")
	rtx.Must(err, "Could not parse URL")
	retiredFile, err = content.FromURL(context.Background(), u)
	rtx.Must(err, "Could not create content.Provider")
}

func TestDoRequest(t *testing.T) {
	setUp()
	ctx := context.Background()
	site.LoadFrom(ctx, localRawfile, retiredFile)

	expectedJson := `{"AnnotatorDate":"2018-12-05T00:00:00Z","Annotations":{"147.1.2.3":{"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822},"Network":{}},"8.8.8.8":{"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822},"Network":{}}}}`
	expectedResp := api.Response{
		AnnotatorDate: time.Date(2018, 12, 5, 0, 0, 0, 0, time.UTC),
		Annotations: map[string]*types.Annotations{
			"147.1.2.3": &types.Annotations{
				Geo:     &types.GeolocationIP{ContinentCode: "NA", CountryCode: "US", CountryName: "United States", Latitude: 37.751, Longitude: -97.822},
				Network: &types.ASData{},
			},
			"8.8.8.8": &types.Annotations{
				Geo:     &types.GeolocationIP{ContinentCode: "NA", CountryCode: "US", CountryName: "United States", Latitude: 37.751, Longitude: -97.822},
				Network: &types.ASData{},
			},
			"64.86.148.132": &types.Annotations{
				Geo: &types.GeolocationIP{ContinentCode: "NA", CountryCode: "US", City: "New York", Latitude: 40.7667, Longitude: -73.8667},
				Network: &types.ASData{
					ASNumber: 0x1935,
					ASName:   "TATA COMMUNICATIONS (AMERICA) INC",
					Systems: []types.System{
						{ASNs: []uint32{0x1935}},
					},
				},
			},
		},
	}
	callCount := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCount < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			fmt.Fprint(w, expectedJson)
		}
		callCount++
	}))
	url := ts.URL

	//url = "https://annotator-dot-mlab-sandbox.appspot.com/batch_annotate"
	ips := []string{"8.8.8.8", "147.1.2.3", "64.86.148.132"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := api.GetAnnotations(ctx, url, time.Now(), ips, "reqInfo")
	if err == nil {
		t.Fatal("Should have timed out")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := api.GetAnnotations(ctx, url, time.Now(), ips, "reqInfo")
	if err != nil {
		t.Fatal(err)
	}

	if callCount != 4 {
		t.Error("Should have been two calls to server.")
	}

	if diff := deep.Equal(expectedResp, *resp); diff != nil {
		t.Error(diff)
	}
}

func TestSomeErrors(t *testing.T) {
	callCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCount == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "body message")
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		callCount++
	}))
	url := ts.URL

	ips := []string{"8.8.8.8", "147.1.2.3"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := api.GetAnnotations(ctx, url, time.Now(), ips, "reqInfo")
	if callCount != 1 {
		t.Errorf("Should have been 1 call to server: %d", callCount)
	}
	if err == nil {
		t.Fatal("Should have produced an error")
	}
	if !strings.Contains(err.Error(), "body message") {
		t.Error("Expected err containing body message", err)
	}
	if !strings.Contains(err.Error(), "Internal Server Error") {
		t.Error("Expected err containing Internal Server Error", err)
	}
}
