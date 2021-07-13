package api_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	types "github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/annotation-service/site"
	"github.com/m-lab/go/content"
	"github.com/m-lab/go/rtx"
	uuid "github.com/m-lab/uuid-annotator/annotator"
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
				Geo: &types.GeolocationIP{
					ContinentCode: "NA",
					CountryCode:   "US",
					CountryName:   "United States",
					Latitude:      37.751,
					Longitude:     -97.822,
				},
				Network: &types.ASData{},
			},
			"8.8.8.8": &types.Annotations{
				Geo: &types.GeolocationIP{ContinentCode: "NA",
					CountryCode: "US",
					CountryName: "United States",
					Latitude:    37.751,
					Longitude:   -97.822,
				},
				Network: &types.ASData{},
			},
			// Verify current ipv4 sites are annotated correctly.
			"64.86.148.132": &types.Annotations{
				Geo: &types.GeolocationIP{
					ContinentCode: "NA",
					CountryCode:   "US",
					City:          "New York",
					Latitude:      40.7667,
					Longitude:     -73.8667,
				},
				Network: &types.ASData{
					CIDR:     "64.86.148.128/26",
					ASNumber: 0x1935,
					ASName:   "TATA COMMUNICATIONS (AMERICA) INC",
					Systems: []types.System{
						{ASNs: []uint32{0x1935}},
					},
				},
			},
			// Verify current ipv6 sites are annotated correctly.
			"2001:5a0:4300::132": &types.Annotations{
				Geo: &types.GeolocationIP{
					ContinentCode: "NA",
					CountryCode:   "US",
					City:          "New York",
					Latitude:      40.7667,
					Longitude:     -73.8667,
				},
				Network: &types.ASData{
					CIDR:     "2001:5a0:4300::/64",
					ASNumber: 0x1935,
					ASName:   "TATA COMMUNICATIONS (AMERICA) INC",
					Systems: []types.System{
						{ASNs: []uint32{0x1935}},
					},
				},
			},
			// Verify that retired sites are annotated correctly.
			"196.201.2.198": &types.Annotations{
				Geo: &types.GeolocationIP{ContinentCode: "AF",
					CountryCode: "GH",
					City:        "Accra",
					Latitude:    5.606,
					Longitude:   -0.1681,
				},
				Network: &types.ASData{
					CIDR:     "196.201.2.192/26",
					ASNumber: 0x7915,
					ASName:   "Ghana Internet Exchange Association",
					Systems: []types.System{
						{ASNs: []uint32{0x7915}},
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
	ips := []string{"8.8.8.8", "147.1.2.3", "64.86.148.132", "2001:5a0:4300::132", "196.201.2.198"}
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

func TestConvertAnnotationsToServerAnnotations(t *testing.T) {
	a := &types.Annotations{
		Geo: &types.GeolocationIP{
			ContinentCode:       "NA",
			CountryCode:         "US",
			Subdivision1ISOCode: "NY",
			Subdivision1Name:    "New York",
			City:                "New York",
			PostalCode:          "10011",
			Latitude:            1.2,
			Longitude:           2.3,
			AccuracyRadiusKm:    1,
			Missing:             false,
		},
		Network: &types.ASData{
			CIDR:     "192.168.0.0/26",
			ASNumber: 10,
			ASName:   "fake AS name",
			Missing:  false,
			Systems: []types.System{
				{ASNs: []uint32{10}},
			},
		},
	}
	empty := &types.Annotations{
		Geo: &types.GeolocationIP{
			Missing: true,
		},
		Network: &types.ASData{
			Missing: true,
		},
	}
	expectedServer := &uuid.ServerAnnotations{
		// NOTE: the Site and Machine fields will not be specified.
		Geo: &uuid.Geolocation{
			ContinentCode:       "NA",
			CountryCode:         "US",
			Subdivision1ISOCode: "NY",
			Subdivision1Name:    "New York",
			City:                "New York",
			PostalCode:          "10011",
			Latitude:            1.2,
			Longitude:           2.3,
			AccuracyRadiusKm:    1,
			Missing:             false,
		},
		Network: &uuid.Network{
			CIDR:     "192.168.0.0/26",
			ASNumber: 10,
			ASName:   "fake AS name",
			Missing:  false,
			Systems: []uuid.System{
				{ASNs: []uint32{10}},
			},
		},
	}
	expectedEmpty := &uuid.ServerAnnotations{
		Geo: &uuid.Geolocation{
			Missing: true,
		},
		Network: &uuid.Network{
			Missing: true,
		},
	}
	expectedClient := &uuid.ClientAnnotations{
		Geo: &uuid.Geolocation{
			ContinentCode:       "NA",
			CountryCode:         "US",
			Subdivision1ISOCode: "NY",
			Subdivision1Name:    "New York",
			City:                "New York",
			PostalCode:          "10011",
			Latitude:            1.2,
			Longitude:           2.3,
			AccuracyRadiusKm:    1,
			Missing:             false,
		},
		Network: &uuid.Network{
			CIDR:     "192.168.0.0/26",
			ASNumber: 10,
			ASName:   "fake AS name",
			Missing:  false,
			Systems: []uuid.System{
				{ASNs: []uint32{10}},
			},
		},
	}

	gs := api.ConvertAnnotationsToServerAnnotations(a)
	if !reflect.DeepEqual(gs, expectedServer) {
		t.Errorf("ConvertAnnotationsToServerAnnotations() = %v, want %v", gs, expectedServer)
	}
	gempty := api.ConvertAnnotationsToServerAnnotations(empty)
	if !reflect.DeepEqual(gempty, expectedEmpty) {
		t.Errorf("ConvertAnnotationsToServerAnnotations() = %v, want %v", gempty, expectedEmpty)
	}

	gc := api.ConvertAnnotationsToClientAnnotations(a)
	if !reflect.DeepEqual(gc, expectedClient) {
		t.Errorf("ConvertAnnotationsToServerAnnotations() = %v, want %v", gc, expectedClient)
	}
}
