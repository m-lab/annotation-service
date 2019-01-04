package wrapper_test

import (
	"errors"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geoloader"
	"github.com/m-lab/annotation-service/geoloader/internal/wrapper"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

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
	aw := wrapper.New()

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
	if aw.SetAnnotator(nil, fakeErr) != wrapper.ErrGoroutineNotOwner {
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

func race(t *testing.T, wg *sync.WaitGroup, aw *wrapper.AnnWrapper, fake *fakeAnnotator, stop chan struct{}) {
	defer wg.Done()
	for {
		ann, err := aw.GetAnnotator()
		if err == nil && ann == nil {
			t.Fatal(err)
			return
		}

		aw.GetLastUsed()

		aw.Unload()

		ann, err = aw.GetAnnotator()
		if err == wrapper.ErrNilEntry {
			if aw.ReserveForLoad() {
				time.Sleep(time.Millisecond)
				// This may sometimes fail if another goroutine has called unload.
				err := aw.SetAnnotator(fake, nil)
				if err != nil {
					if err == wrapper.ErrGoroutineNotOwner {
						// Annotator was unloaded by another goroutine.
					} else {
						t.Fatal(err)
						return
					}
				}
			}
		}

		select {
		case <-stop:
			return
		default:
		}
	}
}

// This test should be run with -race
func TestRaces(t *testing.T) {
	aw := wrapper.New()
	done := make(chan struct{})
	wg := sync.WaitGroup{}

	fake := fakeAnnotator{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go race(t, &wg, &aw, &fake, done)
	}

	time.Sleep(2 * time.Second)
	close(done)
	wg.Wait()
	log.Println(fake.unloadCount)

}