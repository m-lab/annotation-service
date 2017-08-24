package handler_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

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
