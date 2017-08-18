package loader_test

import (
	"log"
	"testing"

	"google.golang.org/appengine/aetest"
	
	"github.com/m-lab/annotation-service/loader"
)

func TestCreateZipReader(t *testing.T) {
	// TODO: add code to disable in travis
	ctx, done, err := aetest.NewContext()
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create aecontext")
	}
	defer done()
	zipReader, err := loader.CreateZipReader(ctx, "test-annotator-sandbox", "MaxMind/2017/08/15/GeoLite2City.zip")
	if err != nil {
		log.Println(err)
		t.Errorf("Failed to create zipReader")
	}

	if len(zipReader.File) != 3 {
		t.Errorf("wrong number of files", len(zipReader.File))
	}
}
