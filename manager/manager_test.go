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
		t.Error("Should be", manager.ErrPendingAnnotatorLoad)
	}

	_, err = am.GetAnnotator(names[1])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Error("Should be", manager.ErrPendingAnnotatorLoad)
	}

	_, err = am.GetAnnotator(names[2])
	if err != manager.ErrPendingAnnotatorLoad {
		t.Error("Should be", manager.ErrPendingAnnotatorLoad)
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

	ann, err := am.GetAnnotator(names[0])
	if err != nil {
		t.Error("Not expecting:", err)
	}
	if ann == nil {
		t.Error("Expecting non-nil annotator")
	}

	// Now try to load 2 more.  The second one should cause an eviction.
	wg = &sync.WaitGroup{}
	wg.Add(2)
	go func(date string) {
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(date) {
			time.Sleep(3 * time.Millisecond)
		}
		wg.Done()
	}(names[2])
	go func(date string) {
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(date) {
			time.Sleep(3 * time.Millisecond)
		}
		wg.Done()
	}(names[3])
	wg.Wait()

	// Loading two more will have caused one to be evicted, so exactly one of these
	// should no longer be loaded, and return an ErrPendingAnnotatorLoad.
	_, err0 := am.GetAnnotator(names[0])
	_, err1 := am.GetAnnotator(names[1])
	switch {
	case err0 == nil && err1 == nil:
		t.Error("One of the items should have been evicted")
	case err0 == nil && err1 == manager.ErrPendingAnnotatorLoad:
		// Good
	case err0 == manager.ErrPendingAnnotatorLoad && err1 == nil:
		// Good
	default:
		t.Error("Should have had exactly one ErrPending...", err0, err1)
	}
}

func TestE2ELoadMultipleDataset(t *testing.T) {
	manager.InitDataset()

	tests := []struct {
		ip   string
		time string
		res  string
	}{
		{"1.4.128.0", "1199145600", `annotator is loading`},
		{"1.9.128.0", "1512086400", `annotator is loading`},
		{"1.22.128.0", "1512086400", `annotator is loading`},
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

	time.Sleep(30 * time.Second)

	tests2 := []struct {
		ip   string
		time string
		res  string
	}{
		{"1.4.128.0", "1199145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":{}}`},
		{"1.9.128.0", "1512086400", `{"Geo":{"continent_code":"AS","country_code":"MY","country_name":"Malaysia","region":"14","city":"Kuala Lumpur","postal_code":"50400","latitude":3.149,"longitude":101.697},"ASN":{}}`},
		{"1.22.128.0", "1512086400", `{"Geo":{"continent_code":"AS","country_code":"IN","country_name":"India","region":"DL","city":"Delhi","postal_code":"110062","latitude":28.6667,"longitude":77.2167},"ASN":{}}`},
	}
	for _, test := range tests2 {
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
