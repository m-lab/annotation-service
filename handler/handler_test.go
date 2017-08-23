package handler_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/handler"
)

func TestAnnotate(t *testing.T) {
	tests := []struct {
		ip   string
		time string
		res  string
	}{
		{"1.4.128.0", "625600", `{"Geo":{"city": "Not A Real City", "postal_code":"10583"},"ASN":{}}`},
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
		res *handler.RequestData
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
			res: &handler.RequestData{"127.0.0.1", 4, time.Unix(10, 0)},
			err: nil,
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=2620:0:1003:1008:5179:57e3:3c75:1886&since_epoch=10", nil),
			res: &handler.RequestData{"2620:0:1003:1008:5179:57e3:3c75:1886", 6, time.Unix(10, 0)},
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
