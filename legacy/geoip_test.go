package legacy_test

import (
	"testing"

	"github.com/m-lab/annotation-service/legacy"
)

func TestOpenAndFree(t *testing.T) {
	file := "./testdata/GeoLiteCity.dat"

	gi, err := legacy.Open(file, "GeoLiteCity.dat")

	if err != nil {
		t.Error(err)
	}

	if gi == nil {
		t.Fatal("legacy file not loaded")
	}
	gi.Free()
}
