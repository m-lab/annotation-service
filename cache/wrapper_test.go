package cache_test

import (
	"errors"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/cache"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

type fakeObject struct {
	lock        sync.Mutex
	unloadCount int
}

func free(object interface{}) {
	obj := object.(*fakeObject)
	obj.lock.Lock()
	obj.unloadCount++
	obj.lock.Unlock()
}

func load() (interface{}, error) {
	return &fakeObject{}, nil
}

// This exercises all the basic functions of the cache.TestAnnWrapper
func TestAnnWrapper(t *testing.T) {
	aw := cache.NewEntry(load, free)

	if !aw.GetLastUsed().Equal(time.Time{}) {
		t.Error("incorrect last used")
	}

	// Check status.
	_, err := aw.Get()
	if err != cache.ErrObjectUnloaded {
		t.Error(err)
	}

	// Should do nothing.
	err = aw.Unload()
	if err != nil {
		t.Error(err)
	}

	fakeErr := errors.New("FakeError")
	if aw.Set(nil, fakeErr) != cache.ErrGoroutineNotOwner {
		t.Error("Should have failed to set annotator")
	}

	// Clear the error with an unload
	err = aw.Unload()
	if err != nil {
		t.Error(err)
	}

	if !aw.ReserveForLoad() {
		t.Fatal("didn't get reservation")
	}
	if aw.ReserveForLoad() {
		t.Fatal("shouldn't have gotten reservation")
	}

	err = aw.Set(nil, fakeErr)
	if err != nil {
		t.Error("Should have succeeded:", err)
	}

	// Just check the status to see that fakeErr is returned.
	_, err = aw.Get()
	if err != fakeErr {
		t.Error(err)
	}

	// Should be able to get the reservation for loading.
	if !aw.ReserveForLoad() {
		t.Fatal("didn't get reservation")
	}
	// Attempt to get the annotator should give loading error status.
	_, err = aw.Get()
	if err != cache.ErrObjectLoading {
		t.Error(err)
	}

	fake := fakeObject{}

	err = aw.Set(&fake, nil)
	if err != nil {
		t.Error(err)
	}

	// There should now be a valid annotator, and this should update the lastUsed time.
	updateTime := time.Now()
	obj, err := aw.Get()
	if err != nil {
		t.Error(err)
	}
	if obj != &fake {
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
	err = aw.Unload()
	if err != nil {
		t.Fatal(err)
	}

	if !aw.GetLastUsed().Equal(time.Time{}) {
		t.Error("incorrect last used")
	}

	// Now we should be able to get a reservation again.
	if !aw.ReserveForLoad() {
		t.Fatal("didn't get reservation")
	}

	// Check that Unload was actually called.
	if fake.unloadCount != 1 {
		t.Error("Should have called Unload once", fake.unloadCount)
	}
}

// This is a helper function that exercises loading and unloading annotator.
func race(t *testing.T, wg *sync.WaitGroup, aw *cache.Entry, fake *fakeObject, stop chan struct{}) {
	defer wg.Done()
	for {
		// Get the annotator.  May be nil, but should not error.
		ann, err := aw.Get()
		if err == nil && ann == nil {
			t.Fatal(err)
			return
		}

		// Test getting the lastUsed time.
		aw.GetLastUsed()

		// Unload the annotator.  It may or may not be loaded, and typically this is done
		// by the eviction code.
		aw.Unload()

		// Get the annotator again.  Might be loaded or not, depending on activity of other goroutines.
		ann, err = aw.Get()
		// If the cache state is ErrNilEntry, then it is eligible to be loaded.
		if err == cache.ErrObjectUnloaded {
			// Try to get the reservation.
			if aw.ReserveForLoad() {
				// If we got the reservation, then wait a brief time and try to set the annotator.
				time.Sleep(time.Millisecond)

				// This should always succeed.
				err := aw.Set(fake, nil)
				if err != nil {
					t.Error(err)
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

// TestRaces launches 10 goroutines, each of which competes to manipulate the cache,
// by getting the reservation, loading the annotator, setting the annotator, unloading.
// Each goroutine just runs a tight loop.
// This test should be run with -race
func TestRaces(t *testing.T) {
	aw := cache.NewEntry(load, free)
	done := make(chan struct{})
	wg := sync.WaitGroup{}

	fake := fakeObject{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go race(t, &wg, &aw, &fake, done)
	}

	time.Sleep(2 * time.Second)
	close(done)
	wg.Wait()
	log.Println(fake.unloadCount)

}
