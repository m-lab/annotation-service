package manager_test

import (
	"runtime"
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

	// HACK - wait until the two goroutines complete.
	// Not sure if this is stable across implementations.
	// Wait for goroutines to complete.
	for runtime.NumGoroutine() > 3 {
	}

	ann, err = am.GetAnnotator("20110102")
	if err != nil {
		t.Error("Not expecting:", err)
	}
	if ann == nil {
		t.Error("Expecting non-nil annotator")
	}
	if runtime.NumGoroutine() != 3 {
		t.Error(runtime.NumGoroutine())
	}
}
