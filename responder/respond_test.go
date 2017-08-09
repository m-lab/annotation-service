package responder_test

import (
	"testing"
	"net/http"
	"net/http/httptest"
	"net/url"
	
	"github.com/m-lab/annotation-service/responder"

)

func TestRequest(t *testing.T){
		tests := []struct {
		ip       string
		time     string
		res      string
		time_num int64
	}{
		{"1.4.128.0", "625600", "[\n  {\"ip\": \"1.4.128.0\", \"type\": \"STRING\"},\n  {\"country\": \"Thailand\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"TH\", \"type\": \"STRING\"},\n]", 625600},
		{"1.32.128.1", "625600", "[\n  {\"ip\": \"1.32.128.1\", \"type\": \"STRING\"},\n  {\"country\": \"Singapore\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"SG\", \"type\": \"STRING\"},\n]", 625600},
		{"2001:502:100e::", "625600", "[\n  {\"ip\": \"2001:502:100e::\", \"type\": \"STRING\"},\n  {\"country\": \"N/A\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"US\", \"type\": \"STRING\"},\n]", 625600},
		{"2001:504:13:ffff:ffff:ffff:ffff:ffff", "625600", "[\n  {\"ip\": \"2001:504:13:ffff:ffff:ffff:ffff:ffff\", \"type\": \"STRING\"},\n  {\"country\": \"N/A\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"US\", \"type\": \"STRING\"},\n]", 625600},
		{"MEMEMEME", "625600", "Invalid request", 625600},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()
		r := &http.Request{}
		r.URL, _ = url.Parse("/annotate?ip_addr=" + url.QueryEscape(test.ip) + "&since_epoch=" + url.QueryEscape(test.time))
		responder.Annotate(w, r)
		body := w.Body.String()
		if string(body) != test.res {
			t.Errorf("\nGot\n__%s__\nexpected\n__%s__\n", body, test.res)
		}
	}
}
