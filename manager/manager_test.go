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

	"github.com/m-lab/annotation-service/geoloader"

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
	log.Println("Fake loader loading", date)
	time.Sleep(2 * time.Millisecond)
	log.Println("Fake loader done loading", date)
	return &geolite2.GeoDataset{}, nil
}

func TestAnnotatorCache(t *testing.T) {
	am := manager.NewAnnotatorCache(3, 2, 50*time.Millisecond, fakeLoader)
	names := []string{"Maxmind/2018/01/01/20180101T054119Z-GeoLite2-City-CSV.zip",
		"Maxmind/2018/01/02/20180201T054119Z-GeoLite2-City-CSV.zip",
		"Maxmind/2018/01/03/20180301T054119Z-GeoLite2-City-CSV.zip",
		"Maxmind/2018/01/04/20180401T054119Z-GeoLite2-City-CSV.zip",
		"Maxmind/2018/01/05/20180501T054119Z-GeoLite2-City-CSV.zip"}

	// These are all fake names.
	_, err := am.GetAnnotator(names[0])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Fatal("Got", err)
	}

	_, err = am.GetAnnotator(names[1])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Fatal("Got", err)
	}

	// This one should NOT kick off a load, because numPending already max.
	_, err = am.GetAnnotator(names[2])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Fatal("Got", err)
	}

	// Wait for both annotator to be available.  These will also prevent
	// eviction
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

	// Make the last access on names[1] 10 msec later.
	time.Sleep(10 * time.Millisecond)
	ann, err = am.GetAnnotator(names[1])
	if err != nil {
		t.Error("Not expecting:", err)
	}
	if ann == nil {
		t.Error("Expecting non-nil annotator")
	}

	// Verify that third is NOT available.  This kicks off loading.
	// TODO - this is flaky
	_, err = am.GetAnnotator(names[2])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Fatal("Got", err)
	}

	// Wait until it has loaded.
	func(key string) {
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(key) {
			time.Sleep(1 * time.Millisecond)
		}
	}(names[2])

	// This should fail, because there are no slots available.
	_, err = am.GetAnnotator(names[3])
	if err != manager.ErrAnnotatorCacheFull {
		t.Fatal("Got", err)
	}

	// Wait for a 4th to be loaded, which requires one to be evicted.
	// names[0] should be evicted first, since it is the least recently used.
	func(key string) {
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(key) {
			time.Sleep(1 * time.Millisecond)
		}
	}(names[3])

	// Now one of the originals should have been evicted.
	var err0, err1, err2 error
	_, err0 = am.GetAnnotator(names[0])
	_, err1 = am.GetAnnotator(names[1])
	_, err2 = am.GetAnnotator(names[2])

	switch {
	case err0 == nil && err1 == nil && err2 == nil:
		t.Error("One of the items should have been evicted", err0, err1, err2)
	case err0 != nil:
		if err1 != nil || err2 != nil {
			t.Error("More than one nil:", err0, ":", err1, ":", err2)
		}
	case err1 != nil:
		if err0 != nil || err2 != nil {
			t.Error("More than one nil:", err0, ":", err1, ":", err2)
		}
	case err2 != nil:
		if err0 != nil || err1 != nil {
			t.Error("More than one nil:", err0, ":", err1, ":", err2)
		}
	default:
		t.Error("Should have exactly one ErrPending:", err0, ":", err1, ":", err2)
	}
}

func TestE2ELoadMultipleDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test that uses GCS")
	}
	manager.InitAnnotatorCache()
	geoloader.UpdateArchivedFilenames()

	tests := []struct {
		ip   string
		time string
		res  string
	}{
		// This request needs a legacy binary dataset
		// TODO uncomment when legacy is working again.
		// {"1.4.128.0", "1199145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":null}`},
		// If more recent dataset is used, we get:
		// {"Geo":{"continent_code":"AS","country_code":"TH","country_name":"Thailand","region":"45","city":"Roi Et","postal_code":"45000","latitude":16.0533,"longitude":103.6539},"ASN":null}'},
		// This request needs a geolite2 dataset
		{"1.9.128.0", "1512086400", `{"Geo":{"continent_code":"AS","country_code":"MY","country_name":"Malaysia","region":"14","city":"Kuala Lumpur","postal_code":"50400","latitude":3.149,"longitude":101.697},"ASN":null}`},
		// This request needs the latest dataset in the memory.
		{"1.22.128.0", "1544400000", `{"Geo":{"continent_code":"AS","country_code":"IN","country_name":"India","region":"HR","city":"Gurgaon","postal_code":"122017","latitude":28.4667,"longitude":77.0333},"ASN":null}`},
		// This request used a loaded & removed legacy dataset.
		//{"1.4.128.0", "1199145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":null}`},
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

		if string(body) != test.res {
			t.Errorf("\nGot\n__%s__\nexpected\n__%s__\n", body, test.res)
		}
	}
}
