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
	geoloader.UseOnlyMarchForTest()
	// Load the small directory.
	manager.UpdateDirectory()

	tests := []struct {
		ip   string
		time string
		res  string
	}{
		// This request needs a legacy binary dataset
		{"1.4.128.0", "1199145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":null}`},
		// This request needs another legacy binary dataset
		// `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"77","city":"Bung","postal_code":"37000","latitude":15.695,"longitude":104.648},"ASN":null}`
		{"1.4.128.0", "1399145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":null}`},
		// This request needs a geolite2 dataset
		{"1.9.128.0", "1512086400", `{"Geo":{"continent_code":"AS","country_code":"MY","country_code3":"MYS","country_name":"Malaysia","region":"14","city":"Kuala Lumpur","postal_code":"50586","latitude":3.167,"longitude":101.7},"ASN":null}`},
		// This request needs the latest dataset in the memory.
		{"1.22.128.0", "1544400000", `{"Geo":{"continent_code":"AS","country_code":"IN","country_name":"India","region":"HR","city":"Faridabad","latitude":28.4333,"longitude":77.3167},"ASN":null}`},
		// This request used a loaded & removed legacy dataset.
		//{"1.4.128.0", "1199145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":null}`},
	}
	for n, test := range tests {
		w := httptest.NewRecorder()
		r := &http.Request{}
		r.URL, _ = url.Parse("/annotate?ip_addr=" + url.QueryEscape(test.ip) + "&since_epoch=" + url.QueryEscape(test.time))
		log.Println("Calling handler")
		handler.Annotate(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Error("Failed annotation for", test.ip)
			continue
		}

		body := w.Body.String()
		log.Println(body)

		if string(body) != test.res {
			t.Errorf("%d:\nGot\n__%s__\nexpected\n__%s__\n", n, body, test.res)
		}
	}
}
