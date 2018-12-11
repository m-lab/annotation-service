package manager_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/manager"
)

func fakeLoader(date string) (api.Annotator, error) {
	return &geolite2.GeoDataset{}, nil
}

func TestAnnotatorMap(t *testing.T) {
	am := manager.NewAnnotatorMap(fakeLoader)

	ann, err := am.GetAnnotator("Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip")
	if err != manager.ErrPendingAnnotatorLoad {
		t.Error("Should be", manager.ErrPendingAnnotatorLoad)
	}

	ann, err = am.GetAnnotator("Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip")
	if err != manager.ErrPendingAnnotatorLoad {
		t.Error("Should be", manager.ErrPendingAnnotatorLoad)
	}

	// Wait for both annotator to be available.
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func(date string) {
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(date) {
		}
		wg.Done()
	}("Maxmind/2018/09/12/20180912T054119Z-GeoLite2-City-CSV.zip")
	go func(date string) {
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(date) {
		}
		wg.Done()
	}("Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip")
	wg.Wait()

	ann, err = am.GetAnnotator("Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip")
	if err != nil {
		t.Error("Not expecting:", err)
	}
	if ann == nil {
		t.Error("Expecting non-nil annotator")
	}
}
