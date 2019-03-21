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

	once    sync.Once // This is used to construct the builder the first time it is used.
	builder *listBuilder
)

// SetDirectory wraps the list of annotators in a Directory, and safely replaces the global
// annotatorDirectory.
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

// MustUpdateDirectory loads ALL datasets into memory.
// NOTE: This may log.Fatal if there is a problem constructing the Directory.
// TODO rename Directory and this function
func MustUpdateDirectory() {
	once.Do(func() {
		v4loader := geoloader.LegacyV4Loader(legacy.LoadAnnotator)
		v6loader := geoloader.LegacyV6Loader(legacy.LoadAnnotator)
		g2loader := geoloader.Geolite2Loader(geolite2.LoadGeolite2)

		builder = newListBuilder(v4loader, v6loader, g2loader)
		if builder == nil {
			// This only happens if one of the loaders is nil.
			log.Fatal("Nil list builder")
		}
	})
	err := builder.update()
	if err != nil {
		// TODO - add a metric?
		log.Println(err)
	}
	combo := builder.build()

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

// newListBuilder initializes a listBuilder object, and preloads the CachingLoaders.
// The arguments must all be non-nil, or the return value will be nil.
func newListBuilder(v4, v6, g2 api.CachingLoader) *listBuilder {
	if v4 == nil || v6 == nil || g2 == nil {
		return nil
	}
	return &listBuilder{legacyV4: v4, legacyV6: v6, geolite2: g2}
}

// Update updates the (dynamic) CachingLoaders
func (bldr *listBuilder) update() error {
	var errV4, errV6, errG2 error

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		errV4 = bldr.legacyV4.UpdateCache()
		wg.Done()
	}()
	go func() {
		errV6 = bldr.legacyV6.UpdateCache()
		wg.Done()
	}()
	go func() {
		errG2 = bldr.geolite2.UpdateCache()
		wg.Done()
	}()
	wg.Wait()

	if errV4 != nil {
		return errV4
	}
	if errV6 != nil {
		return errV6
	}
	if errG2 != nil {
		return errG2
	}
	return nil
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
	}

	// Now append the Geolite2 annotators
	g2 := directory.SortSlice(bldr.geolite2.Fetch())

	combo := make([]api.Annotator, 0, len(g2)+len(legacy))
	combo = append(combo, legacy...)
	combo = append(combo, g2...)

	if len(combo) < 1 {
		log.Println("No annotators available")
		return nil
	}

	return combo
}
