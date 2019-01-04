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

// This exercises all the basic functions of the wrapper.TestAnnWrapper
// TODO - the next PR should add a test that checks for concurrency correctness and races.
func TestAnnWrapper(t *testing.T) {
	aw := geoloader.NewAnnWrapper()

	if !aw.GetLastUsed().Equal(time.Time{}) {
		t.Error("incorrect last used")
	}

	// Check status.
	_, err := aw.GetAnnotator()
	if err != geoloader.ErrNilEntry {
		t.Error(err)
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

	err = aw.SetAnnotator(nil, fakeErr)
	if err != nil {
		t.Error("Should have succeeded:", err)
	}

	// Just check the status to see that fakeErr is returned.
	_, err = aw.GetAnnotator()
	if err != fakeErr {
		t.Error(err)
	}

	// Should be able to get the reservation for loading.
	if !aw.ReserveForLoad() {
		t.Fatal("didn't get reservation")
	}
	// Attempt to get the annotator should give loading error status.
	_, err = aw.GetAnnotator()
	if err != geoloader.ErrAnnotatorLoading {
		t.Error(err)
	}

	fakeAnn := fakeAnnotator{}

	err = aw.SetAnnotator(&fakeAnn, nil)
	if err != nil {
		t.Error(err)
	}

	// There should now be a valid annotator, and this should update the lastUsed time.
	updateTime := time.Now()
	ann, err := aw.GetAnnotator()
	if err != nil {
		t.Error(err)
	}
	if ann != &fakeAnn {
		t.Error("Annotator not as expected")
	}

	// The GetAnnotator call should have updated the lastUsed time.
	if aw.GetLastUsed().Before(updateTime) {
		t.Error("last used should be close to now", aw.GetLastUsed(), updateTime)
	}

	// Since annotator is valid, we shouldn't be able to reserve for load.
	if aw.ReserveForLoad() {
		t.Fatal("shouldn't have gotten reservation")
	}

	// Now unload the annotator.
	aw.Unload()
	if !aw.GetLastUsed().Equal(time.Time{}) {
		t.Error("incorrect last used")
	}

	// Now we should be able to get a reservation again.
	if !aw.ReserveForLoad() {
		t.Fatal("didn't get reservation")
	}

	// Check that Unload was actually called.
	if fakeAnn.unloadCount != 1 {
		t.Error("Should have called Unload once", fakeAnn.unloadCount)
	}
}
