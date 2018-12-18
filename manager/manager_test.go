package manager_test

import (
	"errors"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
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
