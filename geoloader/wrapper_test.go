package geoloader_test

import (
	"errors"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geoloader"
)

type fakeAnnotator struct {
	api.Annotator
	unloadCount int
}

func (f *fakeAnnotator) Unload() {
	f.unloadCount++
}

func TestAnnWrapper(t *testing.T) {
	aw := geoloader.NewAnnWrapper()

	if !aw.GetLastUsed().Equal(time.Time{}) {
		t.Error("incorrect last used")
	}

	updateTime := time.Now()
	aw.UpdateLastUsed()
	if aw.GetLastUsed().Before(updateTime) {
		t.Error("last used should be close to now")
	}

	if aw.Status() != geoloader.ErrNilEntry {
		t.Error(aw.Status())
	}

	// Should do nothing.
	aw.Unload()

	fakeErr := errors.New("FakeError")
	if aw.SetAnnotator(nil, fakeErr) != geoloader.ErrGoroutineNotOwner {
		t.Error("Should have failed to set annotator")
	}
	if !aw.ReserveForLoad() {
		t.Fatal("didn't get reservation")
	}
	if aw.ReserveForLoad() {
		t.Fatal("shouldn't have gotten reservation")
	}

	err := aw.SetAnnotator(nil, fakeErr)
	if err != nil {
		t.Error("Should have succeeded:", err)
	}

	if aw.Status() != fakeErr {
		t.Error(aw.Status())
	}

	if !aw.ReserveForLoad() {
		t.Fatal("didn't get reservation")
	}

	fakeAnn := fakeAnnotator{}

	err = aw.SetAnnotator(&fakeAnn, nil)
	if err != nil {
		t.Error(err)
	}
	if aw.Status() != nil {
		t.Error(aw.Status())
	}
	ann, err := aw.GetAnnotator()
	if err != nil {
		t.Error(err)
	}
	if ann != &fakeAnn {
		t.Error("Annotator not as expected")
	}
	// Now we shouldn't be able to reserve for load.
	if aw.ReserveForLoad() {
		t.Fatal("shouldn't have gotten reservation")
	}
	aw.Unload()
	if !aw.GetLastUsed().Equal(time.Time{}) {
		t.Error("incorrect last used")
	}

	// Now we should be able to get a reservation again.
	if !aw.ReserveForLoad() {
		t.Fatal("didn't get reservation")
	}
}
