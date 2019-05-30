package handler_test

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/geolite2v2"
	"github.com/m-lab/annotation-service/iputils"

	"github.com/go-test/deep"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/manager"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestIp6to4(t *testing.T) {
	ss := handler.Ip6to4("2002:dced:117c::dced:117c")
	if ss != "220.237.17.124" {
		t.Errorf("ip6to4 not done correctly: expect 220.237.17.124 actually " + ss)
	}

	es := handler.Ip6to4("2002:dced")
	if es != "" {
		t.Errorf("ip6to4 not done correctly: expect empty string actually " + es)
	}
}

func TestAnnotate(t *testing.T) {
	tests := []struct {
		ip   string
		time string
		res  string
	}{
		{"1.4.128.0", "625600", `{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583","latitude":42.1,"longitude":-73.1},"Network":null}`},
		{"This will be an error.", "1000", "invalid IP address"},
	}
	// TODO - make and use an annotator generator
	ann := &geolite2v2.GeoDataset{
		Start: time.Now().Truncate(24 * time.Hour),
		IP4Nodes: []geolite2v2.GeoIPNode{
			{
				BaseIPNode: iputils.BaseIPNode{
					IPAddressLow:  net.IPv4(0, 0, 0, 0),
					IPAddressHigh: net.IPv4(255, 255, 255, 255),
				},
				LocationIndex: 0,
				PostalCode:    "10583",
				Latitude:      42.1,
				Longitude:     -73.1,
			},
		},
		IP6Nodes: []geolite2v2.GeoIPNode{
			{
				BaseIPNode: iputils.BaseIPNode{
					IPAddressLow:  net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					IPAddressHigh: net.IP{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				},
				LocationIndex: 0,
				PostalCode:    "10583",
				Latitude:      42.1,
				Longitude:     -73.1,
			},
		},
		LocationNodes: []geolite2v2.LocationNode{
			{
				CityName: "Not A Real City", RegionCode: "ME",
			},
		},
	}
	manager.SetDirectory([]api.Annotator{ann})

	for _, test := range tests {
		w := httptest.NewRecorder()
		r := &http.Request{}
		r.URL, _ = url.Parse("/annotate?ip_addr=" + url.QueryEscape(test.ip) + "&since_epoch=" + url.QueryEscape(test.time))
		handler.Annotate(w, r)
		body := w.Body.String()
		if string(body) != test.res {
			t.Errorf("\nGot\n__%s__\nexpected\n__%s__\n", body, test.res)
		}
	}
}

func TestValidateAndParse(t *testing.T) {
	tests := []struct {
		req *http.Request
		res *api.RequestData
		err error
	}{
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=127.0.0.1&since_epoch=fail", nil),
			res: nil,
			err: errors.New("invalid time"),
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=fail&since_epoch=10", nil),
			res: nil,
			err: errors.New("invalid IP address"),
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=127.0.0.1&since_epoch=10", nil),
			res: &api.RequestData{IP: "127.0.0.1", IPFormat: 4, Timestamp: time.Unix(10, 0)},
			err: nil,
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=2620:0:1003:1008:5179:57e3:3c75:1886&since_epoch=10", nil),
			res: &api.RequestData{IP: "2620:0:1003:1008:5179:57e3:3c75:1886", IPFormat: 6, Timestamp: time.Unix(10, 0)},
			err: nil,
		},
	}
	for _, test := range tests {
		res, err := handler.ValidateAndParse(test.req)
		if diff := deep.Equal(res, test.res); diff != nil {
			t.Error(diff)
		}
		if diff := deep.Equal(err, test.err); diff != nil {
			t.Error(diff)
		}
	}

}

type badReader int

func (badReader) Read(_ []byte) (n int, err error) {
	return 0, errors.New("Bad Reader")
}

func TestBatchValidateAndParse(t *testing.T) {
	timeCon, _ := time.Parse(time.RFC3339, "2002-10-02T15:00:00Z")
	tests := []struct {
		source io.Reader
		res    []api.RequestData
		err    error
	}{
		{
			source: badReader(0),
			res:    nil,
			err:    errors.New("Bad Reader"),
		},
		{
			source: bytes.NewBufferString(`{`),
			res:    nil,
			err:    errors.New("unexpected end of JSON input"),
		},
		{
			source: bytes.NewBufferString(`[]`),
			res:    []api.RequestData{},
			err:    nil,
		},
		{
			source: bytes.NewBufferString(`[{"ip": "Bad IP", "timestamp": "2002-10-02T15:00:00Z"}]`),
			res:    nil,
			err:    errors.New("invalid IP address"),
		},
		{
			source: bytes.NewBufferString(`[{"ip": "127.0.0.1", "timestamp": "2002-10-02T15:00:00Z"},` +
				`{"ip": "2620:0:1003:1008:5179:57e3:3c75:1886", "timestamp": "2002-10-02T15:00:00Z"}]`),
			res: []api.RequestData{
				{IP: "127.0.0.1", IPFormat: 4, Timestamp: timeCon},
				{IP: "2620:0:1003:1008:5179:57e3:3c75:1886", IPFormat: 6, Timestamp: timeCon},
			},
			err: nil,
		},
	}
	for i, test := range tests {
		jsonBuffer, err := ioutil.ReadAll(test.source)
		if err != nil {
			if err.Error() != test.err.Error() {
				log.Printf("Expected %T\n", test.err)
				t.Error(err)
			}
			continue
		}
		res, err := handler.BatchValidateAndParse(jsonBuffer)
		if diff := deep.Equal(res, test.res); diff != nil {
			t.Error(diff)
		}
		// TODO use deep.Equal for testing errors?
		if err != nil && test.err == nil || err == nil && test.err != nil {
			t.Errorf("Test %d: Expected %+v, got %+v.", i, test.err, err)
			continue
		}
		if err != nil && test.err != nil && err.Error() != test.err.Error() {
			t.Errorf("Test %d: Expected %+v, got %+v.", i, test.err, err)
			continue
		}
	}

}

func TestBatchAnnotate(t *testing.T) {
	tests := []struct {
		body string
		res  string
		alt  string // Alternate valid result
	}{
		{
			body: "{",
			res:  "unexpected end of JSON input",
			alt:  "",
		},
		{
			body: `[{"ip": "127.0.0.1", "timestamp": "2017-08-25T13:31:12.149678161-04:00"},
                    {"ip": "2620:0:1003:1008:5179:57e3:3c75:1886", "timestamp": "2017-08-25T14:32:13.149678161-04:00"}]`,
			res: `{"127.0.0.1ov94o0":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583"},"Network":null},"2620:0:1003:1008:5179:57e3:3c75:1886ov97hp":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583"},"Network":null}}`,
			// TODO - remove alt after updating json annotations to omitempty.
			alt: `{"127.0.0.1ov94o0":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"Network":null},"2620:0:1003:1008:5179:57e3:3c75:1886ov97hp":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"Network":null}}`,
		},
	}
	// TODO - make a test utility in geolite2 package.
	ann := &geolite2v2.GeoDataset{
		Start: time.Now().Truncate(24 * time.Hour),
		IP4Nodes: []geolite2v2.GeoIPNode{
			{
				BaseIPNode: iputils.BaseIPNode{
					IPAddressLow:  net.IPv4(0, 0, 0, 0),
					IPAddressHigh: net.IPv4(255, 255, 255, 255),
				},
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		IP6Nodes: []geolite2v2.GeoIPNode{
			{
				BaseIPNode: iputils.BaseIPNode{
					IPAddressLow:  net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					IPAddressHigh: net.IP{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				},
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		LocationNodes: []geolite2v2.LocationNode{
			{
				CityName: "Not A Real City", RegionCode: "ME",
			},
		},
	}
	manager.SetDirectory([]api.Annotator{ann})
	for _, test := range tests {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/batch_annotate", strings.NewReader(test.body))
		handler.BatchAnnotate(w, r)
		body := w.Body.String()
		if string(body) != test.res && string(body) != test.alt {
			t.Errorf("\nGot\n__%s__\nexpected\n__%s__\n", body, test.res)
		}
	}
}

// TODO(JM) Update the test code/data here once we are no longer
// returning a canned response
func TestGetMetadataForSingleIP(t *testing.T) {
	tests := []struct {
		req *api.RequestData
		res api.GeoData
	}{
		{
			req: &api.RequestData{IP: "127.0.0.1", IPFormat: 4, Timestamp: time.Unix(0, 0)},
			res: api.GeoData{
				Geo:     &api.GeolocationIP{City: "Not A Real City", PostalCode: "10583"},
				Network: nil},
		},
	}
	ann := &geolite2v2.GeoDataset{
		Start: time.Now().Truncate(24 * time.Hour),
		IP4Nodes: []geolite2v2.GeoIPNode{
			{
				BaseIPNode: iputils.BaseIPNode{
					IPAddressLow:  net.IPv4(0, 0, 0, 0),
					IPAddressHigh: net.IPv4(255, 255, 255, 255),
				},
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		IP6Nodes: []geolite2v2.GeoIPNode{
			{
				BaseIPNode: iputils.BaseIPNode{
					IPAddressLow:  net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					IPAddressHigh: net.IP{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				},
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		LocationNodes: []geolite2v2.LocationNode{
			{
				CityName: "Not A Real City",
			},
		},
	}
	manager.SetDirectory([]api.Annotator{ann})
	for _, test := range tests {
		res, _ := handler.GetMetadataForSingleIP(test.req)
		if diff := deep.Equal(res, test.res); diff != nil {
			t.Error(diff)
		}
	}
}
