package manager_test

import (
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
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

func TestAnnotatorMap(t *testing.T) {
	manager.MaxPending = 2
	manager.MaxDatasetInMemory = 3

	am := manager.NewAnnotatorMap(fakeLoader)
	names := []string{"Maxmind/2018/01/01/20180101T054119Z-GeoLite2-City-CSV.zip",
		"Maxmind/2018/01/02/20180201T054119Z-GeoLite2-City-CSV.zip",
		"Maxmind/2018/01/03/20180301T054119Z-GeoLite2-City-CSV.zip",
		"Maxmind/2018/01/04/20180401T054119Z-GeoLite2-City-CSV.zip",
		"Maxmind/2018/01/05/20180501T054119Z-GeoLite2-City-CSV.zip"}

	// These are all fake names.
	_, err := am.GetAnnotator(names[0])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Fatal("Should be", manager.ErrPendingAnnotatorLoad)
	}

	_, err = am.GetAnnotator(names[1])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Fatal("Should be", manager.ErrPendingAnnotatorLoad)
	}

	// This one should NOT kick off a load, because numPending already max.
	_, err = am.GetAnnotator(names[2])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Fatal("Should be", manager.ErrPendingAnnotatorLoad)
	}

	// Wait for both annotator to be available.
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func(date string) {
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(date) {
			time.Sleep(3 * time.Millisecond)
		}
		wg.Done()
	}(names[0])
	go func(date string) {
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(date) {
			time.Sleep(3 * time.Millisecond)
		}
		wg.Done()
	}(names[1])
	wg.Wait()

	// Verify that both annotators are now available.
	ann, err := am.GetAnnotator(names[0])
	if err != nil {
		t.Error("Not expecting:", err)
	}
	if ann == nil {
		t.Error("Expecting non-nil annotator")
	}

	ann, err = am.GetAnnotator(names[1])
	if err != nil {
		t.Error("Not expecting:", err)
	}
	if ann == nil {
		t.Error("Expecting non-nil annotator")
	}

	// Verify that third is NOT available.  This kicks off loading.
	_, err = am.GetAnnotator(names[2])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Fatal("Should be", manager.ErrPendingAnnotatorLoad)
	}

	// Wait until it has loaded.
	func(date string) {
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(date) {
			time.Sleep(3 * time.Millisecond)
		}
	}(names[2])

	// And now load the fourth.  This should cause synchronous eviction, and NOT cause loading.
	_, err = am.GetAnnotator(names[3])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Fatal("Should be", manager.ErrPendingAnnotatorLoad)
	}

	// Loading two more will have caused one to be evicted, so exactly one of these
	// should no longer be loaded, and return an ErrPendingAnnotatorLoad.
	// One of these checks will also trigger another load, but that is OK.
	_, err0 := am.GetAnnotator(names[0])
	_, err1 := am.GetAnnotator(names[1])
	_, err2 := am.GetAnnotator(names[2])
	switch {
	case err0 == nil && err1 == nil && err2 == nil:
		t.Error("One of the items should have been evicted")
	case err0 == manager.ErrPendingAnnotatorLoad:
		if err1 != nil || err2 != nil {
			t.Error("More than one nil", err0, err1, err2)
		}
	case err1 == manager.ErrPendingAnnotatorLoad:
		if err0 != nil || err2 != nil {
			t.Error("More than one nil", err0, err1, err2)
		}
	case err2 == manager.ErrPendingAnnotatorLoad:
		if err0 != nil || err1 != nil {
			t.Error("More than one nil", err0, err1, err2)
		}
	default:
		t.Error("Should have had exactly one ErrPending...", err0, err1)
	}
}

func TestE2ELoadMultipleDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test that uses GCS")
	}
	manager.InitDataset()
	manager.MaxDatasetInMemory = 2
	tests := []struct {
		ip   string
		time string
		res  string
	}{
		// This request needs a legacy binary dataset
		//		{"1.4.128.0", "1199145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":{}}`},
		// This request needs another legacy binary dataset
		//		{"1.4.128.0", "1399145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":{}}`},
		// This request needs a geolite2 dataset
		{"1.9.128.0", "1512086400", `{"Geo":{"continent_code":"AS","country_code":"MY","country_name":"Malaysia","region":"14","city":"Kuala Lumpur","postal_code":"50400","latitude":3.149,"longitude":101.697},"ASN":{}}`},
		// This request needs the latest dataset in the memory.
		{"1.22.128.0", "1544400000", `{"Geo":{"continent_code":"AS","country_code":"IN","country_name":"India","region":"HR","city":"Gurgaon","postal_code":"122017","latitude":28.4667,"longitude":77.0333},"ASN":{}}`},
		// This request used a loaded & removed legacy dataset.
		//{"1.4.128.0", "1199145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":{}}`},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()
		r := &http.Request{}
		r.URL, _ = url.Parse("/annotate?ip_addr=" + url.QueryEscape(test.ip) + "&since_epoch=" + url.QueryEscape(test.time))
		log.Println("Calling handler")
		handler.Annotate(w, r)
		i := 30
		for w.Result().StatusCode != http.StatusOK {
			log.Println("Try again", w.Result().Status, w.Body.String())
			i--
			if i == 0 {
				break
			}
			time.Sleep(1 * time.Second)
			w = httptest.NewRecorder()
			handler.Annotate(w, r)
		}

		body := w.Body.String()
		log.Println(body)

		if string(body) != test.res {
			t.Errorf("\nGot\n__%s__\nexpected\n__%s__\n", body, test.res)
		}
	}
}
