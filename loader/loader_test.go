package loader_test

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/m-lab/annotation-service/loader"
)

func TestCreateZipReader(t *testing.T) {
	if os.Getenv("TRAVIS") == "true" {
		log.Println("skipping test")
		return
	}
	zipReader, err := loader.CreateZipReader(context.Background(), "test-annotator-sandbox", "MaxMind/2017/08/15/GeoLite2City.zip")
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create zipReader")
	}

	if len(zipReader.File) != 3 {
		t.Error("wrong number of files", len(zipReader.File))
	}
}
