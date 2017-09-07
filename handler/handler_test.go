package handler_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/etl/schema"
)

func TestAnnotate(t *testing.T) {
	tests := []struct {
		ip   string
		time string
		res  string
	}{
		{"1.4.128.0", "625600", `{"Geo":{"city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"ASN":{}}`},
		{"This will be an error.", "1000", "Invalid request"},
	}
	handler.CurrentGeoDataset = &parser.GeoDataset{
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
		res *schema.RequestData
		err error
	}{
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=127.0.0.1&since_epoch=fail", nil),
			res: nil,
			err: errors.New("Invalid time"),
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=fail&since_epoch=10", nil),
			res: nil,
			err: errors.New("Invalid IP address"),
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=127.0.0.1&since_epoch=10", nil),
			res: &schema.RequestData{"127.0.0.1", 4, time.Unix(10, 0)},
			err: nil,
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=2620:0:1003:1008:5179:57e3:3c75:1886&since_epoch=10", nil),
			res: &schema.RequestData{"2620:0:1003:1008:5179:57e3:3c75:1886", 6, time.Unix(10, 0)},
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
		res    []schema.RequestData
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
			res:    []schema.RequestData{},
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
			res: []schema.RequestData{
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
	}{
		{
			body: "{",
			res:  "Invalid Request!",
		},
		{
			body: `[{"ip": "127.0.0.1", "timestamp": "2017-08-25T13:31:12.149678161-04:00"},
                               {"ip": "2620:0:1003:1008:5179:57e3:3c75:1886", "timestamp": "2017-08-25T13:31:12.149678161-04:00"}]`,
			res: `{"127.0.0.1ov94o0":{"Geo":{"city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"ASN":{}},"2620:0:1003:1008:5179:57e3:3c75:1886ov94o0":{"Geo":{"city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"ASN":{}}}`,
		},
	}
	handler.CurrentGeoDataset = &parser.GeoDataset{
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
	}
	for _, test := range tests {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/batch_annotate", strings.NewReader(test.body))
		handler.BatchAnnotate(w, r)
		body := w.Body.String()
		if string(body) != test.res {
			t.Errorf("\nGot\n__%s__\nexpected\n__%s__\n", body, test.res)
		}
	}
}

// TODO(JM) Update the test code/data here once we are no longer
// returning a canned response
func TestGetMetadataForSingleIP(t *testing.T) {
	tests := []struct {
		req *schema.RequestData
		res *schema.MetaData
	}{
		{
			req: &schema.RequestData{"127.0.0.1", 4, time.Unix(0, 0)},
			res: &schema.MetaData{
				Geo: &schema.GeolocationIP{City: "Not A Real City", Postal_code: "10583"},
				ASN: &schema.IPASNData{}},
		},
	}
	handler.CurrentGeoDataset = &parser.GeoDataset{
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
	}
	for _, test := range tests {
		res := handler.GetMetadataForSingleIP(test.req)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %s, got %s", test.res, res)
		}
	}
}

func TestConvertIPNodeToMetaData(t *testing.T) {
	tests := []struct {
		node parser.IPNode
		locs []parser.LocationNode
		res  *schema.MetaData
	}{
		{
			node: parser.IPNode{LocationIndex: 0, PostalCode: "10583"},
			locs: []parser.LocationNode{{CityName: "Not A Real City"}},
			res: &schema.MetaData{
				Geo: &schema.GeolocationIP{City: "Not A Real City", Postal_code: "10583"},
				ASN: &schema.IPASNData{}},
		},
		{
			node: parser.IPNode{LocationIndex: -1, PostalCode: "10583"},
			locs: nil,
			res: &schema.MetaData{
				Geo: &schema.GeolocationIP{Postal_code: "10583"},
				ASN: &schema.IPASNData{}},
		},
	}
	for _, test := range tests {
		res := handler.ConvertIPNodeToMetaData(test.node, test.locs)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %s, got %s", test.res, res)
		}
	}
}
