package manager_test

import (
	"fmt"
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
	time.Sleep(50 * time.Millisecond)
	return &geolite2.GeoDataset{}, nil
}

func TestAnnotatorMap(t *testing.T) {
	am := manager.NewAnnotatorMap(fakeLoader)

	for i := 0; i < 10; i++ {
		month := i + 10
		dataset := fmt.Sprintf("Maxmind/2018/09/%d/201809%dT054119Z-GeoLite2-City-CSV.zip", month, month)
		log.Println("Attempting to load", dataset)
		_, err := am.GetAnnotator(dataset)
		if i < manager.MaxDatasetInMemory {
			if err != manager.ErrPendingAnnotatorLoad {
				t.Error("Should be", manager.ErrPendingAnnotatorLoad)
			}
		} else {
			if err != manager.ErrTooManyAnnotators {
				t.Error("Should be", manager.ErrTooManyAnnotators, "looking for", dataset)
			}
		}
	}

	time.Sleep(20 * time.Millisecond)
	log.Println("Start additional load attempts")

	// Wait for all annotators to be available.
	wg := &sync.WaitGroup{}
	for i := 0; i < manager.MaxDatasetInMemory; i++ {
		month := i + 10
		dataset := fmt.Sprintf("Maxmind/2018/09/%d/201809%dT054119Z-GeoLite2-City-CSV.zip", month, month)
		wg.Add(1)
		go func(key string) {
			for _, err := am.GetAnnotator(key); err != nil; _, err = am.GetAnnotator(key) {
				log.Println(err, key)
				time.Sleep(1 * time.Millisecond)
			}
			wg.Done()
		}(dataset)
	}

	wg.Wait()

	month := 10 + manager.MaxDatasetInMemory - 1
	dataset := fmt.Sprintf("Maxmind/2018/09/%d/201809%dT054119Z-GeoLite2-City-CSV.zip", month, month)
	ann, err := am.GetAnnotator(dataset)
	if err != nil {
		t.Error("Not expecting:", err, "looking for", dataset)
	}
	if ann == nil {
		t.Error("Expecting non-nil annotator")
	}
}
