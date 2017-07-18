package annotator

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func Test_annotate(t *testing.T) {
	tests := []struct {
		ip       string
		time     string
		res      string
		usestr   bool
		time_num int64
	}{
		{"192.156.789.234", "625600", "", false, 625600},
		{"2620:0:1003:1008:dc7a:13d4:dfb3:d622", "625600", "", false, 625600},
		{"2620:0:1003:1008:DC7A:13D4:DFB3:D622", "625600", "", false, 625600},
		{"2620:0:1003:1008:dC7A:13d4:dfb3:d622", "625600", "", false, 625600},
		{"199.666.666.6666", "0", "NOT A RECOGNIZED IP FORMAT!", true, 0},
		{"199.666.666.66f", "0", "NOT A RECOGNIZED IP FORMAT!", true, 0},
		{"199.666.666.666", "f", "INVALID TIME!", true, 0},
		{"199.666.666.6666", "", "INVALID TIME!", true, 0},
		{"199.666.666.6666", "46d", "INVALID TIME!", true, 0},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()
		//This only works after GO 1.7
		//r := httptest.NewRequest("GET", "/annotate?ip_addr="+url.QueryEscape(test.ip)+"&since_epoch="+url.QueryEscape(test.time), nil)
		//This works for GO 1.6
		r := &http.Request{}
		r.URL, _ = url.Parse("/annotate?ip_addr=" + url.QueryEscape(test.ip) + "&since_epoch=" + url.QueryEscape(test.time))

		i, d := false, false
		metrics_activeRequests = gaugeMock{&i, &d}
		obc := 0
		metrics_requestTimes = summaryMock{&obc}

		annotate(w, r)

		metGauge, _ := metrics_activeRequests.(gaugeMock)
		metSum, _ := metrics_requestTimes.(summaryMock)
		if !(*metGauge.i && *metGauge.d) {
			t.Errorf("DIDN'T DO GAUGE METRICS CORRECTLY %t & %t!", *metGauge.i, *metGauge.d)
		}
		if *metSum.observeCount == 0 {
			t.Error("NEVER CALLED OBSERVE!!")
		}

		body := w.Body.String()
		if test.usestr && string(body) != test.res {
			t.Errorf("Got \"%s\", expected \"%s\".", body, test.res)
		}
	}

}
