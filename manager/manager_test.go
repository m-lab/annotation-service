package manager_test

import (
	"log"
	"sync"
	"testing"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/manager"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func fakeLoader(date string) (api.Annotator, error) {
	return &geolite2.GeoDataset{}, nil
}

func TestAnnotatorMap(t *testing.T) {
	am := manager.NewAnnotatorMap(fakeLoader)

	ann, err := am.GetAnnotator("20110101")
	if err != manager.ErrPendingAnnotatorLoad {
		t.Error("Should be", manager.ErrPendingAnnotatorLoad)
	}

	ann, err = am.GetAnnotator("20110102")
	if err != manager.ErrPendingAnnotatorLoad {
		t.Error("Should be", manager.ErrPendingAnnotatorLoad)
	}

	// Wait for both annotator to be available.
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func(date string) {
		for _, err := am.GetAnnotator(date); err != nil; _, err = am.GetAnnotator(date) {
		}
		wg.Done()
	}("20110101")
	go func(date string) {
		for _, err := am.GetAnnotator(date); err != nil; _, err = am.GetAnnotator(date) {
		}
		wg.Done()
	}("20110102")
	wg.Wait()

	ann, err = am.GetAnnotator("20110102")
	if err != nil {
		t.Error("Not expecting:", err)
	}
	if ann == nil {
		t.Error("Expecting non-nil annotator")
		log.Printf("%+v\n", am)
	}
}
