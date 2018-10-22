package handler_test

import (
	"bytes"
	"errors"
	"io"
	//"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	//"sync"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/common"
	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/parser"
)

func TestAnnotate(t *testing.T) {
	handler.UpdateFilenamelist("downloader-mlab-testing")
	tests := []struct {
		ip   string
		time string
		res  string
	}{
		{"1.4.128.0", "1539704761", `{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583","latitude":42.1,"longitude":-73.1},"ASN":{}}`},
		{"This will be an error.", "1000", "Invalid request"},
	}
	dataset := &parser.GeoDataset{
		IP4Nodes: []parser.IPNode{
			{
				IPAddressLow:  net.IPv4(0, 0, 0, 0),
				IPAddressHigh: net.IPv4(255, 255, 255, 255),
				LocationIndex: 0,
				PostalCode:    "10583",
				Latitude:      42.1,
				Longitude:     -73.1,
			},
		},
		IP6Nodes: []parser.IPNode{
			{
				IPAddressLow:  net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				IPAddressHigh: net.IP{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				LocationIndex: 0,
				PostalCode:    "10583",
				Latitude:      42.1,
				Longitude:     -73.1,
			},
		},
		LocationNodes: []parser.LocationNode{
			{
				CityName: "Not A Real City", RegionCode: "ME",
			},
		},
	}
	handler.CurrentGeoDataset.Init()
	handler.CurrentGeoDataset.SetCurrentDataset(dataset)
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

func TestLegacyDataset(t *testing.T) {
	handler.UpdateFilenamelist("downloader-mlab-testing")
	tests := []struct {
		ip   string
		time string
		res  string
	}{
		{"1.4.128.0", "1199145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":{}}`},
		{"1.5.190.1", "1420070400", `{"Geo":{"continent_code":"AS","country_code":"JP","country_code3":"JPN","country_name":"Japan","region":"40","city":"Tokyo","latitude":35.685,"longitude":139.751},"ASN":{}}`},
		{"1.9.128.0", "1512086400", `{"Geo":{"continent_code":"AS","country_code":"MY","country_name":"Malaysia","region":"14","city":"Kuala Lumpur","postal_code":"50400","latitude":3.149,"longitude":101.697},"ASN":{}}`},
		{"1.22.128.0", "1512086400", `{"Geo":{"continent_code":"AS","country_code":"IN","country_name":"India","region":"DL","city":"Delhi","postal_code":"110062","latitude":28.6667,"longitude":77.2167},"ASN":{}}`},
	}
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
		res *common.RequestData
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
			res: &common.RequestData{"127.0.0.1", 4, time.Unix(10, 0)},
			err: nil,
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=2620:0:1003:1008:5179:57e3:3c75:1886&since_epoch=10", nil),
			res: &common.RequestData{"2620:0:1003:1008:5179:57e3:3c75:1886", 6, time.Unix(10, 0)},
			err: nil,
		},
	}
	for _, test := range tests {
		res, err := handler.ValidateAndParse(test.req)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %+v, got %+v.", test.res, res)
		}
		if !reflect.DeepEqual(err, test.err) {
			t.Errorf("Expected %+v, got %+v.", test.err, err)
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
		res    []common.RequestData
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
			res:    []common.RequestData{},
			err:    nil,
		},
		{
			source: bytes.NewBufferString(`[{"ip": "Bad IP", "timestamp": "2002-10-02T15:00:00Z"}]`),
			res:    nil,
			err:    errors.New("Invalid IP address."),
		},
		{
			source: bytes.NewBufferString(`[{"ip": "127.0.0.1", "timestamp": "2002-10-02T15:00:00Z"},` +
				`{"ip": "2620:0:1003:1008:5179:57e3:3c75:1886", "timestamp": "2002-10-02T15:00:00Z"}]`),
			res: []common.RequestData{
				{"127.0.0.1", 4, timeCon},
				{"2620:0:1003:1008:5179:57e3:3c75:1886", 6, timeCon},
			},
			err: nil,
		},
	}
	for _, test := range tests {
		res, err := handler.BatchValidateAndParse(test.source)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %+v, got %+v.", test.res, res)
		}
		if err != nil && test.err == nil || err == nil && test.err != nil {
			t.Errorf("Expected %+v, got %+v.", test.err, err)
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
			res:  "Invalid Request!",
			alt:  "",
		},
		{
			body: `[{"ip": "127.0.0.1", "timestamp": "2018-12-25T13:31:12.149678161-04:00"},
                               {"ip": "2620:0:1003:1008:5179:57e3:3c75:1886", "timestamp": "2018-12-25T13:31:12.149678161-04:00"}]`,
			res: `{"127.0.0.1pkazc0":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583"},"ASN":{}},"2620:0:1003:1008:5179:57e3:3c75:1886pkazc0":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583"},"ASN":{}}}`,
			// TODO - remove alt after updating json annotations to omitempty.
			alt: `{"127.0.0.1pkazc0":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"ASN":{}},"2620:0:1003:1008:5179:57e3:3c75:1886pkazc0":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"ASN":{}}}`,
		},
	}
	handler.CurrentGeoDataset.SetCurrentDataset(&parser.GeoDataset{
		IP4Nodes: []parser.IPNode{
			{
				IPAddressLow:  net.IPv4(0, 0, 0, 0),
				IPAddressHigh: net.IPv4(255, 255, 255, 255),
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		IP6Nodes: []parser.IPNode{
			{
				IPAddressLow:  net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				IPAddressHigh: net.IP{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		LocationNodes: []parser.LocationNode{
			{
				CityName: "Not A Real City", RegionCode: "ME",
			},
		},
	})
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
		req *common.RequestData
		res *common.GeoData
	}{
		{
			req: &common.RequestData{"127.0.0.1", 4, time.Now()},
			res: &common.GeoData{
				Geo: &common.GeolocationIP{City: "Not A Real City", Postal_code: "10583"},
				ASN: &common.IPASNData{}},
		},
	}
	handler.CurrentGeoDataset.SetCurrentDataset(&parser.GeoDataset{
		IP4Nodes: []parser.IPNode{
			{
				IPAddressLow:  net.IPv4(0, 0, 0, 0),
				IPAddressHigh: net.IPv4(255, 255, 255, 255),
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		IP6Nodes: []parser.IPNode{
			{
				IPAddressLow:  net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				IPAddressHigh: net.IP{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		LocationNodes: []parser.LocationNode{
			{
				CityName: "Not A Real City",
			},
		},
	})
	for _, test := range tests {
		res, err := handler.GetMetadataForSingleIP(test.req)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %v, got %v", test.res, res)
		}
	}
}

func TestConvertIPNodeToGeoData(t *testing.T) {
	tests := []struct {
		node parser.IPNode
		locs []parser.LocationNode
		res  *common.GeoData
	}{
		{
			node: parser.IPNode{LocationIndex: 0, PostalCode: "10583"},
			locs: []parser.LocationNode{{CityName: "Not A Real City", RegionCode: "ME"}},
			res: &common.GeoData{
				Geo: &common.GeolocationIP{City: "Not A Real City", Postal_code: "10583", Region: "ME"},
				ASN: &common.IPASNData{}},
		},
		{
			node: parser.IPNode{LocationIndex: -1, PostalCode: "10583"},
			locs: nil,
			res: &common.GeoData{
				Geo: &common.GeolocationIP{Postal_code: "10583"},
				ASN: &common.IPASNData{}},
		},
	}
	for _, test := range tests {
		res := handler.ConvertIPNodeToGeoData(test.node, test.locs)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %v, got %v", test.res, res)
		}
	}
}
