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
// Finally, we use BuildDirectory to create a Directory based on this list.

import (
	"errors"
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

// Directory maintains a list of Annotators, indexed by date.
type Directory struct {
	// fields are immutable after construction using BuildDirectory()
	startDate  time.Time
	annotators []api.Annotator
}

func daysSince(ref time.Time, date time.Time) int {
	i := int(date.Unix()-ref.Unix()) / (24 * 3600)
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

	index := daysSince(d.startDate, date)
	if index >= len(d.annotators) {
		index = len(d.annotators) - 1
	}
	if d.annotators[index] == nil {
		return nil, ErrNilAnnotator
	}
	return d.annotators[index], nil
}

func (d *Directory) replace(ann api.Annotator) {
	date := ann.AnnotatorDate()

	// Use this for any date strictly after the AnnotatorDate...
	replaceAfter := daysSince(d.startDate, date)
	for i := replaceAfter; i < len(d.annotators); i++ {
		old := d.annotators[i]
		if old == nil {
			d.annotators[i] = ann
		} else {
			oldDate := old.AnnotatorDate()
			if oldDate.Before(date) {
				d.annotators[i] = ann
			}
		}

	}
}

// BuildDirectory builds a Directory object from a list of Annotators.
// TODO - how do we handle multiple lists of Annotators that should be merged?
func BuildDirectory(all []api.Annotator) *Directory {
	start := time.Now()

	for i := range all {
		if all[i] != nil && all[i].AnnotatorDate().Before(start) {
			start = all[i].AnnotatorDate()
		}
	}

	annotators := make([]api.Annotator, daysSince(start, time.Now()))

	dir := Directory{startDate: start, annotators: annotators}
	// NOTE: this would be slightly more efficient if done in reverse order.
	for i := range all {
		if all[i] != nil {
			dir.replace(all[i])
		}
	}

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
func MergeAnnotators(lists [][]api.Annotator) []api.Annotator {
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
