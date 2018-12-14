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
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func fakeLoader(date string) (api.Annotator, error) {
	time.Sleep(1 * time.Millisecond)
	return &geolite2.GeoDataset{}, nil
}

func TestAnnotatorMap(t *testing.T) {
	am := manager.NewAnnotatorMap(fakeLoader)

	for i := 0; i < 10; i++ {
		month := i + 10
		dataset := fmt.Sprint("Maxmind/2018/09/", month, "/20180912T054119Z-GeoLite2-City-CSV.zip")
		log.Println("Loading", dataset)
		_, err := am.GetAnnotator(dataset)
		if month < manager.MaxDatasetInMemory {
			if err != manager.ErrPendingAnnotatorLoad {
				t.Error("Should be", manager.ErrPendingAnnotatorLoad)
			}
		} else {
			if err != manager.ErrTooManyAnnotators {
				t.Error("Should be", manager.ErrTooManyAnnotators)
			}
		}
	}

	// Wait for all annotators to be available.
	wg := &sync.WaitGroup{}
	for i := 0; i < manager.MaxDatasetInMemory; i++ {
		month := i + 10
		dataset := fmt.Sprint("Maxmind/2018/09/", month, "/20180912T054119Z-GeoLite2-City-CSV.zip")
		wg.Add(1)
		go func(date string) {
			for _, err := am.GetAnnotator(date); err != nil; _, err = am.GetAnnotator(date) {
				log.Println(err)
				time.Sleep(1 * time.Millisecond)
			}
			wg.Done()
		}(dataset)
	}

	// Try loading a different one.
	ann, err := am.GetAnnotator("Maxmind/2017/08/30/20170815T200946Z-GeoLite2-City-CSV.zip")
	if err != manager.ErrTooManyAnnotators {
		t.Error("Should be", manager.ErrTooManyAnnotators)
	}

	wg.Wait()

	ann, err = am.GetAnnotator("Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip")
	if err != nil {
		t.Error("Not expecting:", err)
	}
	if ann == nil {
		t.Error("Expecting non-nil annotator")
	}
}
