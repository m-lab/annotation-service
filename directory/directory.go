// Package directory provides the GetAnnotator function, which returns an appropriate annotator for
// requests with a particular target date.
// TODO - rename this cache?  Or is there a better name?
package directory

// A directory entry points to an appropriate CompositeAnnotator.
// The composite annotators will have:
//  1. An ASN annotator
//  2. Either
//     a. A Geolite2 annotator
//     b. A legacy v4 and legacy v6 annotator
//
// Once the ASN annotators are available, we will have a different CA for every date, but
// until then, we only have a different CA for each date where a new v4 or v6, or a new GL2
// annotator is available.
//
// Generator is used to construct directories. It collects lists of Annotator objects for each type of
// annotator from CachingLoader objects.
// It then merges the v4 and v6 annotators into a list of CompositeAnnotators, using MergeAnnotators.
// It then appends all the GeoLite2 annotators to this list.
// Then, we merge the Geo annotation list with the ASN annotator list (when available).

import (
	"errors"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/m-lab/annotation-service/api"
)

var (
	// These errors should never happen, unless there is a bug in our implementation.

	// ErrEmptyDirectory is returned by GetAnnotator if a Directory has no entries.
	ErrEmptyDirectory = errors.New("Directory is empty")
)

// CompositeAnnotator wraps several annotators, and calls to Annotate() are forwarded to all of them.
type CompositeAnnotator struct {
	// latest date of the component annotators.  This is precomputed, and returned by AnnotatorDate()
	latestDate time.Time
	annotators []api.Annotator
}

// Annotate calls each of the wrapped annotators to annotate the ann object.
// See Annotator.Annotate().
// Error handling is currently under development.
func (ca CompositeAnnotator) Annotate(ip string, ann *api.GeoData) error {
	for i := range ca.annotators {
		err := ca.annotators[i].Annotate(ip, ann)
		if err != nil {
			// TODO - don't want to return error if there is another annotator that can do the job.
		}
	}
	return nil
}

// AnnotatorDate returns the date of the most recent wrapped annotator.  Most recent is returned
// as we try to apply the most recent annotators that predate the test we are annotating.  So the
// most recent of all the annotators is the date that should be compared to the test date.
func (ca CompositeAnnotator) AnnotatorDate() time.Time {
	return ca.latestDate
}

// Compute the latest AnnotatorDate() value from a slice of annotators.
func computeLatestDate(annotators []api.Annotator) time.Time {
	t := time.Time{}
	for i := range annotators {
		at := annotators[i].AnnotatorDate()
		if at.After(t) {
			t = at
		}
	}
	return t
}

// String creates a string representation of the CA.
// Base annotators will appear as [YYYYMMDD], and composite annotators as (A1A2), e.g.,
// ([20100102]([20110304][20120506]))
func (ca CompositeAnnotator) String() string {
	result := ""
	for _, c := range ca.annotators {
		if t, ok := c.(CompositeAnnotator); ok {
			result = result + "(" + t.String() + ")"
		} else {
			result = result + c.AnnotatorDate().Format("[20060102]")
		}
	}
	return result
}

// NewCompositeAnnotator creates a new instance wrapping the provided slice. Returns nil if the slice is nil.
func NewCompositeAnnotator(annotators []api.Annotator) api.Annotator {
	if annotators == nil {
		return nil
	}
	ca := CompositeAnnotator{latestDate: computeLatestDate(annotators), annotators: annotators}
	return ca
}

// Directory allows searching a list of annotators
// TODO not crazy about this name.
type Directory struct {
	annotators []api.Annotator
}

var lastLogTime = time.Now()

// GetAnnotator returns an appropriate api.Annotator for a given date.
func (d *Directory) GetAnnotator(date time.Time) (api.Annotator, error) {
	if len(d.annotators) < 1 {
		return nil, ErrEmptyDirectory
	}

	ann := d.lastEarlierThan(date)
	if time.Since(lastLogTime) > 5*time.Minute {
		log.Printf("Using (%s) for %s\n", ann.AnnotatorDate().Format("20060102"), date.Format("20060102"))
		lastLogTime = time.Now()
	}
	return ann, nil
}

// Build builds a Directory object from a list of Annotators.
// TODO - how do we handle multiple lists of Annotators that should be merged?
func Build(all []api.Annotator) *Directory {
	dir := Directory{annotators: SortSlice(all)}
	return &dir
}

// Advance advances to the next date among the list elements.
func advance(lists [][]api.Annotator) ([][]api.Annotator, bool) {
	// Start far in the future.
	date := time.Now().Add(1000000 * time.Hour)
	first := -1
	for l, list := range lists {
		if len(list) > 1 {
			d := list[1].AnnotatorDate()
			if d.Before(date) {
				first = l
				date = d
			}
		}
	}
	if first == -1 {
		return nil, false
	}

	// Now advance any list that has the same target date.
	for l, list := range lists {
		if len(list) > 1 && list[1].AnnotatorDate().Equal(date) {
			lists[l] = list[1:]
		}
	}
	return lists, true
}

// MergeAnnotators merges multiple lists of annotators, and returns a list of CompositeAnnotators.
// Result will include a separate CompositeAnnotator for each unique date in any list, and each
// CA will include the most recent annotator from each list, prior to or equal to the CA date.
func MergeAnnotators(lists ...[]api.Annotator) []api.Annotator {
	listCount := len(lists)
	if listCount == 0 {
		return nil
	}
	if listCount == 1 {
		return lists[0]
	}

	// This is an arbitrary size, sufficient to reduce number of reallocations.
	groups := make([][]api.Annotator, 0, 100)

	// For each step, add a group, then advance the list(s) with earliest dates at second index.
	for more := true; more; {
		// Create and add group with first annotator from each list
		group := make([]api.Annotator, len(lists))
		for l, list := range lists {
			if len(list) == 0 {
				return nil
			}
			group[l] = list[0]
		}
		groups = append(groups, group)
		// Advance the lists that have earliest next elements.
		lists, more = advance(lists)
	}

	result := make([]api.Annotator, len(groups))
	for i, group := range groups {
		result[i] = NewCompositeAnnotator(group)
	}
	return result
}

// TODO move all of this to geoloader.
func lessFunc(s []api.Annotator) func(i, j int) bool {
	return func(i, j int) bool {
		ti := s[i].AnnotatorDate()
		tj := s[j].AnnotatorDate()
		return ti.Before(tj)
	}
}

// SortSlice sorts a slice of annotators in date order.
func SortSlice(annotators []api.Annotator) []api.Annotator {
	sort.Slice(annotators, lessFunc(annotators))
	return annotators
}

// Satisfies sort.Search.  Returns index of first annotator that has
// AnnotatorDate() >= date.
func searchFunc(s []api.Annotator, date time.Time) func(i int) bool {
	return func(i int) bool {
		ti := s[i].AnnotatorDate()
		return !ti.Before(date)
	}
}

// Returns the last annotator that has AnnotatorDate < date.  If there is none, then
// it returns the first annotator.  If there are no annotators, it returns nil
func (d *Directory) lastEarlierThan(date time.Time) api.Annotator {
	if len(d.annotators) == 0 {
		return nil
	}

	index := sort.Search(len(d.annotators), searchFunc(d.annotators, date))
	if index == 0 {
		return d.annotators[index]
	}
	return d.annotators[index-1]
}

/*************************************************************************
*                          Directory Builder                             *
*************************************************************************/

// Generator wraps a set of CachingLoaders, and creates a set of merged Annotators on request.
// TODO - not crazy about this name.
type Generator struct {
	legacyV4 api.CachingLoader // loader for legacy v4 annotators
	legacyV6 api.CachingLoader // loader for legacy v6 annotators
	geolite2 api.CachingLoader // loader for geolite2 annotators
	asn      api.CachingLoader // loader for asn annotators (currently nil)
}

// NewGenerator initializes a Generator object, and preloads the CachingLoaders
// TODO New shouldn't do loading.
func NewGenerator(v4, v6, g2 api.CachingLoader) *Generator {
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
	return &Generator{v4, v6, g2, nil}
}

// Update updates the (dynamic) CachingLoaders
func (gen *Generator) Update() error {
	// v4 and v6 are static, so we  don't have to reload them.
	return gen.geolite2.UpdateCache()
}

// Generate creates a complete list of CompositeAnnotators from the cached annotators
// from the CachingLoaders.
func (gen *Generator) Generate() []api.Annotator {
	v4 := SortSlice(gen.legacyV4.Fetch())
	v6 := SortSlice(gen.legacyV6.Fetch())

	var legacy []api.Annotator
	if len(v4)*len(v6) < 1 {
		log.Println("empty legacy v4 or v6 annotator list - skipping legacy")
		legacy = make([]api.Annotator, 0)
	} else {
		legacy = MergeAnnotators(v4, v6)
		// TODO logAnnotatorDates("legacy", legacy)
	}

	// Now append the Geolite2 annotators
	g2 := SortSlice(gen.geolite2.Fetch())

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
