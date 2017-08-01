package annotator

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

//test request syntax validation
func TestValidate(t *testing.T) {
	tests := []struct {
		ip       string
		time     string
		res      string
		time_num int64
	}{
		{"1.10.128.0", "625600", "", 625600},
		{"2620:0:1003:1008:dc7a:13d4:dfb3:d622", "625600", "", 625600},
		{"2620:0:1003:1008:DC7A:13D4:DFB3:D622", "625600", "", 625600},
		{"2620:0:1003:1008:dC7A:13d4:dfb3:d622", "625600", "", 625600},
		{"199.666.666.6666", "0", "NOT A RECOGNIZED IP FORMAT!", 0},
		{"199.666.666.66f", "0", "NOT A RECOGNIZED IP FORMAT!", 0},
		{"199.666.666.666", "f", "INVALID TIME!", 0},
		{"199.666.666.6666", "", "INVALID TIME!", 0},
		{"199.666.666.6666", "46d", "INVALID TIME!", 0},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()

		r := &http.Request{}
		r.URL, _ = url.Parse("/annotate?ip_addr=" + url.QueryEscape(test.ip) + "&since_epoch=" + url.QueryEscape(test.time))

		i, d := false, false
		metrics_activeRequests = gaugeMock{&i, &d}
		obc := 0
		metrics_requestTimes = summaryMock{&obc}

		validate(w, r)

		metGauge, _ := metrics_activeRequests.(gaugeMock)
		metSum, _ := metrics_requestTimes.(summaryMock)
		if !(*metGauge.i && *metGauge.d) {
			t.Errorf("DIDN'T DO GAUGE METRICS CORRECTLY %t & %t!", *metGauge.i, *metGauge.d)
		}
		if *metSum.observeCount == 0 {
			t.Error("NEVER CALLED OBSERVE!!")
		}

		body := w.Body.String()
		if string(body) != test.res {
			t.Errorf("Got \"%s\", expected \"%s\".", body, test.res)
		}
	}

}

func TestAnnotation(t *testing.T) {
	tests := []struct {
		ip       string
		time     string
		res      string
		time_num int64
	}{
		{"254.4.128.0", "625600", "[\n  {\"ip\": \"1.4.128.0\", \"type\": \"STRING\"},\n  {\"country\": \"Thailand\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"TH\", \"type\": \"STRING\"},\n]", 625600},
		{"254.32.128.1", "625600", "[\n  {\"ip\": \"1.32.128.1\", \"type\": \"STRING\"},\n  {\"country\": \"Singapore\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"SG\", \"type\": \"STRING\"},\n]", 625600},
		{"MEMEMEME", "625600", "NOT A RECOGNIZED IP FORMAT!", 625600},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()

		r := &http.Request{}
		r.URL, _ = url.Parse("/annotate?ip_addr=" + url.QueryEscape(test.ip) + "&since_epoch=" + url.QueryEscape(test.time))

		annotate(w, r)

		body := w.Body.String()

		if string(body) != test.res {
			t.Errorf("\nGot\n__%s__\nexpected\n__%s__\n", body, test.res)
		}
	}
}
