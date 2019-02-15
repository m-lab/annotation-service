// Package manager provides interface between handler and lower level implementation
// such as geoloader.
package manager

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/geoloader"
	"github.com/m-lab/annotation-service/legacy"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/directory"
)

var (
	ErrDirectoryIsNil = errors.New("annotatorDirectory has not been initialized")

	// dirLock must be held when accessing or replacing annotatorDirectory.
	dirLock sync.RWMutex
	// annotatorDirectory points to a Directory containing CompositeAnnotators.
	annotatorDirectory *directory.Directory

	genOnce   sync.Once
	generator *directory.Generator
)

func SetDirectory(annotators []api.Annotator) {
	dirLock.Lock()
	defer dirLock.Unlock()
	log.Println("Directory has", len(annotators), "entries")
	annotatorDirectory = directory.Build(annotators)
	if annotatorDirectory == nil {
		log.Println("ERROR LOADING DIRECTORY")
	}
}

// GetAnnotator returns the correct annotator to use for a given timestamp.
func GetAnnotator(date time.Time) (api.Annotator, error) {
	dirLock.RLock()
	defer dirLock.RUnlock()
	if annotatorDirectory == nil {
		return nil, ErrDirectoryIsNil
	}
	return annotatorDirectory.GetAnnotator(date)
}

// Writes list of annotator dates to log, preceeded by header string.
func logAnnotatorDates(header string, an []api.Annotator) {
	b := strings.Builder{}
	b.WriteString(header + "\n")
	for i := range an {
		fmt.Fprintf(&b, "%s\n", an[i].AnnotatorDate().Format("20060102"))
	}
	log.Println(b.String())
}

// UpdateDirectory loads ALL datasets into memory.
// TODO rename Directory and this function
func UpdateDirectory() {
	genOnce.Do(func() {
		v4loader := geoloader.LegacyV4Loader(legacy.LoadAnnotator)
		v6loader := geoloader.LegacyV6Loader(legacy.LoadAnnotator)
		g2loader := geoloader.Geolite2Loader(geolite2.LoadGeolite2)

		generator = directory.NewGenerator(v4loader, v6loader, g2loader)
	})

	err := generator.Update()
	if err != nil {
		// TODO - add a metric?
		log.Println(err)
	}
	combo := generator.Generate()

	// Sort them just in case there are some out of order.
	combo = directory.SortSlice(combo)
	logAnnotatorDates("combo", combo)

	if len(combo) < 1 {
		log.Fatal("No annotators.  Terminating!!")
	}

	SetDirectory(combo)
}
