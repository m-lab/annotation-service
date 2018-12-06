package manager_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/m-lab/annotation-service/manager"
)

func TestAnnotatorMap(t *testing.T) {
	am := manager.NewAnnotatorMap()

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
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(date) {
		}
		wg.Done()
	}("20110101")
	go func(date string) {
		err := errors.New("start")
		for ; err != nil; _, err = am.GetAnnotator(date) {
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
	}
}
