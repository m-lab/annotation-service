package annotator

import (
	"net/http/httptest"
	"net/url"
	"testing"
)

func Test_lookupAndRespond(t *testing.T) {
	tests := []struct {
		ip   string
		time int64
		res  string
	}{
		{"192.156.789.234", 625600, "I got ip 192.156.789.234 and time since epoch 625600."},
		{"192.156.789.234", -625600, "I got ip 192.156.789.234 and time since epoch -625600."},
		{"192.156.789.234", 0, "I got ip 192.156.789.234 and time since epoch 0."},
		{"", 0, "I got ip  and time since epoch 0."},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()
		lookupAndRespond(w, test.ip, test.time)
		body := w.Body.String()
		if string(body) != test.res {
			t.Errorf("Got \"%s\", wanted \"%s\"!", body, test.res)
		}
	}

}

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
		r := httptest.NewRequest("GET", "/annotate?ip_addr="+url.QueryEscape(test.ip)+"&since_epoch="+url.QueryEscape(test.time), nil)
		annotate(w, r)
		body := w.Body.String()
		if test.usestr && string(body) != test.res {
			t.Errorf("Got \"%s\", expected \"%s\".", body, test.res)
		}
		if !test.usestr {
			tw := httptest.NewRecorder()
			lookupAndRespond(tw, test.ip, test.time_num)
			tbody := tw.Body.String()
			if string(body) != string(tbody) {
				t.Errorf("Got \"%s\", expected \"%s\".", body, tbody)
			}
		}
	}

}
