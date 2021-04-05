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

	"github.com/m-lab/annotation-service/asn"
	"github.com/m-lab/annotation-service/geolite2v2"

	"github.com/m-lab/annotation-service/geoloader"
	"github.com/m-lab/annotation-service/legacy"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/directory"
)

var (
	// ErrDirectoryIsNil is returned before the annotatorDirectory is initialized.
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
		log.Print("annotatorDirectory is nil!")
		return nil, ErrDirectoryIsNil
	}
	return annotatorDirectory.GetAnnotator(date)
}

// Writes list of annotator dates to log, preceded by header string.
// This was previously used to log all the annotator dates in MustUpdateDirectory.
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
		g2loader := geoloader.Geolite2Loader(geolite2v2.LoadG2)
		asnv4Loader := geoloader.ASNv4Loader(asn.LoadASNDataset)
		asnv6Loader := geoloader.ASNv6Loader(asn.LoadASNDataset)

		builder = newListBuilder(v4loader, v6loader, g2loader, asnv4Loader, asnv6Loader)
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
	mutex    sync.Mutex        // Prevents concurrent update and/or build
	legacyV4 api.CachingLoader // loader for legacy v4 annotators
	legacyV6 api.CachingLoader // loader for legacy v6 annotators
	geolite2 api.CachingLoader // loader for geolite2 annotators
	asnV4    api.CachingLoader // loader for asn v4 annotators
	asnV6    api.CachingLoader // loader for asn v6 annotators
}

// newListBuilder initializes a listBuilder object, and preloads the CachingLoaders.
// The arguments must all be non-nil, or the return value will be nil.
func newListBuilder(v4, v6, g2, asnV4, asnV6 api.CachingLoader) *listBuilder {
	if v4 == nil || v6 == nil || g2 == nil || asnV4 == nil || asnV6 == nil {
		return nil
	}
	return &listBuilder{legacyV4: v4, legacyV6: v6, geolite2: g2, asnV4: asnV4, asnV6: asnV6}
}

// Update updates the (dynamic) CachingLoaders
func (bldr *listBuilder) update() error {
	bldr.mutex.Lock()
	defer bldr.mutex.Unlock()

	var errV4, errV6, errG2, errAsnV4, errAsnV6 error

	log.Println("Updating dataset directory")
	wg := sync.WaitGroup{}
	wg.Add(5)
	go func() {
		errV4 = bldr.legacyV4.UpdateCache()
		log.Println("Legacy V4 loading done.")
		wg.Done()
	}()
	go func() {
		errV6 = bldr.legacyV6.UpdateCache()
		log.Println("Legacy V6 loading done.")
		wg.Done()
	}()
	go func() {
		errG2 = bldr.geolite2.UpdateCache()
		log.Println("Geolite2 loading done.")
		wg.Done()
	}()
	go func() {
		errAsnV4 = bldr.asnV4.UpdateCache()
		log.Println("ASN V4 loading done.")
		wg.Done()
	}()
	go func() {
		errAsnV6 = bldr.asnV6.UpdateCache()
		log.Println("ASN V6 loading done.")
		wg.Done()
	}()
	wg.Wait()

	log.Println("Dataset update complete.")

	if errV4 != nil {
		return errV4
	}
	if errV6 != nil {
		return errV6
	}
	if errG2 != nil {
		return errG2
	}
	if errAsnV4 != nil {
		return errAsnV4
	}
	if errAsnV6 != nil {
		return errAsnV6
	}
	return nil
}

// build creates a complete list of CompositeAnnotators from the cached annotators
// from the CachingLoaders.
func (bldr *listBuilder) build() []api.Annotator {
	bldr.mutex.Lock()
	defer bldr.mutex.Unlock()

	// merge the legacy V4 & V6 annotators
	legacy := mergeV4V6(bldr.legacyV4.Fetch(), bldr.legacyV6.Fetch(), "legacy")
	// Now append the Geolite2 annotators
	g2 := directory.SortSlice(bldr.geolite2.Fetch())
	geo := make([]api.Annotator, 0, len(g2)+len(legacy))
	geo = append(geo, legacy...)
	geo = append(geo, g2...)
	// here we have all the geo annotators in the ordered list.
	// now merge the ASN V4 & V6 annotators
	asn := mergeV4V6(bldr.asnV4.Fetch(), bldr.asnV6.Fetch(), "ASN")
	// and now we need to create the composite annotators. First list is the
	// geo annotators, the second is the ASN
	combo := directory.MergeAnnotators(geo, asn)

	if len(combo) < 1 {
		log.Println("No annotators available")
		return nil
	}

	return combo
}

// mergeV4V6 holds common logic to merge legacy location and ASN v4 and v6 annotators into composite annotators.
// The purpose of the merge is to fallback to IPv6 lookup if IPv4 lookup was unsuccessful.
func mergeV4V6(v4Annotators, v6Annotators []api.Annotator, discriminator string) []api.Annotator {
	v4 := directory.SortSlice(v4Annotators)
	v6 := directory.SortSlice(v6Annotators)
	var merged []api.Annotator
	if len(v4)*len(v6) < 1 {
		log.Printf("empty v4 or v6 annotator list for %s data, skipping", discriminator)
		merged = make([]api.Annotator, 0)
	} else {
		merged = directory.MergeAnnotators(v4, v6)
	}
	return merged
}
