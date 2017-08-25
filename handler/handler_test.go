package handler_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/handler"
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
			source: bytes.NewBufferString(`[{"ip": "Bad IP", "unix_ts": 100}]`),
			res:    nil,
			err:    errors.New("Invalid IP address."),
		},
		{
			source: bytes.NewBufferString(`[{"ip": "127.0.0.1", "unix_ts": 100},` +
				`{"ip": "2620:0:1003:1008:5179:57e3:3c75:1886", "unix_ts":666}]`),
			res: []schema.RequestData{
				{"127.0.0.1", 4, time.Unix(100, 0)},
				{"2620:0:1003:1008:5179:57e3:3c75:1886", 6, time.Unix(666, 0)},
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
			body: `[{"ip": "127.0.0.1", "unix_ts": 100980},
                               {"ip": "2620:0:1003:1008:5179:57e3:3c75:1886", "unix_ts":666}]`,
			res: `{"127.0.0.125x0":{"Geo":{"city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"ASN":{}},"2620:0:1003:1008:5179:57e3:3c75:1886ii":{"Geo":{"city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"ASN":{}}}`,
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

func TestGetMetadataForSingleIP(t *testing.T) {
	tests := []struct {
		req *schema.RequestData
		res *schema.MetaData
	}{
		{
			req: nil,
			res: &schema.MetaData{
				Geo: &schema.GeolocationIP{City: "Not A Real City", Postal_code: "10583"},
				ASN: &schema.IPASNData{}},
		},
	}

	for _, test := range tests {
		res := handler.GetMetadataForSingleIP(test.req)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %s, got %s", test.res, res)
		}
	}
}
