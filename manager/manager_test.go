package manager_test

import (
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/geoloader"
	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/manager"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

func fakeLoader(date string) (api.Annotator, error) {
	time.Sleep(10 * time.Millisecond)
	return &geolite2.GeoDataset{}, nil
}

func TestInitDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test that uses GCS")
	}
	// Make the dataset filters much more restrictive to prevent OOM and make test faster.
	year, month, day := "(2018|2015)", "03", "(01|08)"
	geoloader.UseSpecificGeolite2DateForTesting(&year, &month, &day)
	year, month, day = "2018", "03", "(01|08)"
	geoloader.UseSpecificASNDateForTesting(&year, &month, &day)

	// Load the small directory.
	manager.MustUpdateDirectory()

	tests := []struct {
		ip   string
		time string
		res  string
	}{
		// Triggers legacy geo 2015-03-08  and ASN 2018-03-01
		{"1.4.128.0", "1377820800", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","latitude":13.75,"longitude":100.467},"Network":{"Systems":[{"ASNs":[23969]}]}}`},
		// Triggers legacy geo 2015-03-08  and ASN 2018-03-01
		{"1.9.128.0", "1512086400", `{"Geo":{"continent_code":"AS","country_code":"MY","country_code3":"MYS","country_name":"Malaysia","region":"05","city":"Seremban","postal_code":"70400","latitude":2.749,"longitude":101.943},"Network":{"Systems":[{"ASNs":[4788]}]}}`},
		// Triggers geolite2 geo 2018-03-08  and ASN 2018-03-08
		{"1.22.128.0", "1544400000", `{"Geo":{"continent_code":"AS","country_code":"IN","country_name":"India","region":"HR","city":"Faridabad","latitude":28.4333,"longitude":77.3167},"Network":{"Systems":[{"ASNs":[45528]}]}}`},
	}
	for n, test := range tests {
		w := httptest.NewRecorder()
		r := &http.Request{}
		r.URL, _ = url.Parse("/annotate?ip_addr=" + url.QueryEscape(test.ip) + "&since_epoch=" + url.QueryEscape(test.time))
		handler.Annotate(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Error("Failed annotation for", test.ip)
			continue
		}

		body := w.Body.String()

		if string(body) != test.res {
			t.Errorf("%d:\nGot\n__%s__\nexpected\n__%s__\n", n, body, test.res)
		}
	}
}
