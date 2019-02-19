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

// InitDataset loads ALL datasets into memory.
// TODO - this will probably OOM when called a second time, since it will load all
// the annotators again.
// TODO - refactor this into parts in geoloader and directory.
// TODO - rename LoadAnnotatorDirectory
func InitDataset() {
	wg := sync.WaitGroup{}
	wg.Add(3)
	var v4 []api.Annotator
	var v6 []api.Annotator
	var g2 []api.Annotator

	go func() {
		var err error
		v4, err = geoloader.LoadAllLegacyV4(legacy.LoadAnnotator)
		if err != nil {
			// This is pretty severe, but we work around most of these failures down below.
			log.Println(err)
		}
		v4 = directory.SortSlice(v4)
		wg.Done()
	}()
	go func() {
		var err error
		v6, err = geoloader.LoadAllLegacyV6(legacy.LoadAnnotator)
		if err != nil {
			log.Println(err)
		}
		v6 = directory.SortSlice(v6)
		wg.Done()
	}()
	go func() {
		var err error
		g2, err = geoloader.LoadAllGeolite2(geolite2.LoadGeolite2)
		if err != nil {
			log.Println(err)
		}
		g2 = directory.SortSlice(g2)
		wg.Done()
	}()

	wg.Wait()

	// Construct the CompositeAnnotators to handle legacy v4/v6
	logAnnotatorDates("v4", v4)
	logAnnotatorDates("v6", v6)
	var legacy []api.Annotator
	if len(v4)*len(v6)*len(g2) < 1 {
		log.Println("empty legacy v4 or v6 annotator list - skipping legacy")
		legacy = make([]api.Annotator, 0)
	} else {
		legacy = directory.MergeAnnotators(v4, v6)
		logAnnotatorDates("legacy", legacy)
	}

	// Now append the Geolite2 annotators
	combo := make([]api.Annotator, 0, len(g2)+len(legacy))
	combo = append(combo, legacy...)
	combo = append(combo, g2...)

	// Sort them just in case there are some out of order.
	combo = directory.SortSlice(combo)
	logAnnotatorDates("combo", combo)

	if len(combo) < 1 {
		log.Fatal("No annotators.  Terminating!!")
	}

	SetDirectory(combo)
}

/*************************************************************************
*                    CompositeAnnotator List Builder                     *
*************************************************************************/

// listBuilder wraps a set of CachingLoaders, and creates a set of merged Annotators on request.
// TODO - unit tests?
type listBuilder struct {
	legacyV4 api.CachingLoader // loader for legacy v4 annotators
	legacyV6 api.CachingLoader // loader for legacy v6 annotators
	geolite2 api.CachingLoader // loader for geolite2 annotators
	asn      api.CachingLoader // loader for asn annotators (currently nil)
}

// newListBuilder initializes a listBuilder object, and preloads the CachingLoaders
// TODO New shouldn't do loading.
func newListBuilder(v4, v6, g2 api.CachingLoader) *listBuilder {
	if v4 == nil || v6 == nil || g2 == nil {
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		v4.UpdateCache()
		wg.Done()
	}()
	go func() {
		v6.UpdateCache()
		wg.Done()
	}()
	go func() {
		g2.UpdateCache()
		wg.Done()
	}()
	wg.Wait()
	return &listBuilder{v4, v6, g2, nil}
}

// Update updates the (dynamic) CachingLoaders
func (bldr *listBuilder) update() error {
	// v4 and v6 are static, so we  don't have to reload them.
	return bldr.geolite2.UpdateCache()
}

// build creates a complete list of CompositeAnnotators from the cached annotators
// from the CachingLoaders.
func (bldr *listBuilder) build() []api.Annotator {
	v4 := directory.SortSlice(bldr.legacyV4.Fetch())
	v6 := directory.SortSlice(bldr.legacyV6.Fetch())

	var legacy []api.Annotator
	if len(v4)*len(v6) < 1 {
		log.Println("empty legacy v4 or v6 annotator list - skipping legacy")
		legacy = make([]api.Annotator, 0)
	} else {
		legacy = directory.MergeAnnotators(v4, v6)
		// TODO logAnnotatorDates("legacy", legacy)
	}

	// Now append the Geolite2 annotators
	g2 := directory.SortSlice(bldr.geolite2.Fetch())

	combo := make([]api.Annotator, 0, len(g2)+len(legacy))
	combo = append(combo, legacy...)
	combo = append(combo, g2...)

	// logAnnotatorDates("combo", combo)

	if len(combo) < 1 {
		log.Println("No annotators available")
		return nil
	}

	return combo
}
