package manager_test

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2v2"
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
	return &geolite2v2.GeoDataset{}, nil
}

func TestInitDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test that uses GCS")
	}

	// Make the dataset filters much more restrictive to prevent OOM and make test faster.
	//geoloader.UseOnlyMarchForTest()
	year, month, day := "(2018|2017|2015|2014)", "03", "(07|08)"
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
		// This request needs a legacy binary dataset
		{"1.4.128.0", "1199145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"Network":{"Systems":[{"ASNs":[23969]}]}}`},
		// This request needs another legacy binary dataset
		{"1.4.128.0", "1399145600",
			`{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"Network":{"Systems":[{"ASNs":[23969]}]}}`},
		// This request needs a geolite2 dataset
		{"1.9.128.0", "1512086400",
			`{"Geo":{"continent_code":"AS","country_code":"MY","country_code3":"MYS","country_name":"Malaysia","region":"14","city":"Kuala Lumpur","postal_code":"50586","latitude":3.167,"longitude":101.7},"Network":{"Systems":[{"ASNs":[4788]}]}}`},
		// This request needs the latest dataset in the memory.
		{"1.22.128.0", "1544400000",
			`{"Geo":{"continent_code":"AS","country_code":"IN","country_name":"India","region":"HR","city":"Faridabad","latitude":28.4333,"longitude":77.3167},"Network":{"Systems":[{"ASNs":[45528]}]}}`},
		{"2002:dced:117c::dced:117c", "1559227976",
			`{"Geo":{"continent_code":"OC","country_code":"AU","country_name":"Australia","region":"VIC","city":"East Malvern","postal_code":"3145","latitude":-37.8833,"longitude":145.05},"Network":{"Systems":[{"ASNs":[4804]}]}}`},
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
	PrintMemUsage()
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
