package handler_test

import (
	"testing"
	"net/http"
	"net/http/httptest"
	"net/url"
	
	"github.com/m-lab/annotation-service/handler"

)

func TestRequest(t *testing.T){
		tests := []struct {
		ip       string
		time     string
		res      string
		time_num int64
	}{
		{"1.4.128.0", "625600", "[\n  {\"ip\": \"1.4.128.0\", \"type\": \"STRING\"},\n  {\"country\": \"Thailand\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"TH\", \"type\": \"STRING\"},\n]", 625600},
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