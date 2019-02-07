// Package directory provides the GetAnnotator function, which returns an appropriate annotator for
// requests with a particular target date.
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
// To construct the directory, we begin with lists of Annotator objects for each type of annotation.
// We first merge the v4 and v6 annotators into a list of CompositeAnnotators, using MergeAnnotators.
// We then append all the GeoLite2 annotators to this list.
// Then, we merge the Geo annotation list with the ASN annotator list.
// Finally, we use Build to create a Directory based on this list.

// Example use (simplified, with some functions that don't exist yet):
// v4, _ = geoloader.LoadLegacyV4(nil)
// v6, _ = geoloader.LoadLegacyV6(nil)
// legacy := directory.MergeAnnotators(v4, v6)  // Creates annotators that will handle v4 or v6
// g2, _ = geoloader.LoadGeolite2(nil)
// combo := make([]api.Annotator, len(g2)+len(legacy))
// combo = append(combo, g2...)
// combo = append(combo, legacy...)
// annotatorDirectory = directory.Build(combo)

// TODO delete this line.  Just here to allow comments in #198

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/m-lab/annotation-service/api"
)

var (
	// These errors should never happen, unless there is a bug in our implementation.

	// ErrEmptyDirectory is returned by GetAnnotator if a Directory has no entries.
	ErrEmptyDirectory = errors.New("Directory is empty")
	// ErrNilAnnotator is returned if GetAnnotator encounters a nil Directory entry.
	ErrNilAnnotator = errors.New("Annotator is nil")
)

// Directory allows searching a list of annotators
type Directory struct {
	annotators []api.Annotator
}

func daysSince(ref time.Time, date time.Time) int {
	i := int((date.Unix() - ref.Unix()) / (24 * 3600))
	if i < 0 {
		return 0
	}
	return i
}

// GetAnnotator returns an appropriate api.Annotator for a given date.
func (d *Directory) GetAnnotator(date time.Time) (api.Annotator, error) {
	if len(d.annotators) < 1 {
		return nil, ErrEmptyDirectory
	}

	ann := d.lastEarlierThan(date)
	log.Printf("Using (%s) for %s\n", ann.AnnotatorDate().Format("20060102"), date.Format("20060102"))
	return ann, nil
}

// Dump prints a summary of the directory to the log.
func (d *Directory) Dump() {
	b := strings.Builder{}
	b.WriteString("Directory:\n")
	for i := range d.annotators {
		fmt.Fprintf(&b, "%s\n", d.annotators[i].AnnotatorDate().Format("20060102"))
	}
	log.Println(b.String())
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

// MergeAnnotators merges multiple lists of annotators, and returns a list of CompositeAnnotators, each
// containing an appropriate annotator from each list.
func MergeAnnotators(lists ...[]api.Annotator) []api.Annotator {
	listCount := len(lists)
	if listCount == 0 {
		return nil
	}
	if listCount == 1 {
		return lists[0]
	}

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
		result[i] = api.NewCompositeAnnotator(group)
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
