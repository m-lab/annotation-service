package manager

import (
	"errors"
	"sync"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geoloader"
)

var (
	// ErrNilDataset is returned when CurrentAnnotator is nil.
	ErrNilDataset = errors.New("CurrentAnnotator is nil")

	// A mutex to make sure that we are not reading from the CurrentAnnotator
	// pointer while trying to update it
	currentDataMutex = &sync.RWMutex{}

	// CurrentAnnotator points to a GeoDataset struct containing the absolute
	// latest data for the annotator to search and reply with
	CurrentAnnotator api.Annotator
)

// GetAnnotator returns the correct annotator to use for a given timestamp.
func GetAnnotator(date time.Time) api.Annotator {
	// dateString := strconv.FormatInt(date.Unix(), encodingBase)
	if date.After(geoloader.LatestDatasetDate) {
		currentDataMutex.RLock()
		ann := CurrentAnnotator
		currentDataMutex.RUnlock()
		return ann
	}
	filename, err := geoloader.SelectArchivedDataset(date)

	if err != nil {
		return nil
	}
	if geoloader.GeoLite2Regex.MatchString(filename) {
		return geoloader.GetArchivedAnnotator(filename)
	} else {
		// TODO return a pointer to legacy dataset
		return nil
	}
}

// InitDataset will update the filename list of archived dataset in memory
// and load the latest Geolite2 dataset in memory.
func InitDataset() {
        geoloader.UpdateArchivedFilenames()

	currentDataMutex.Lock()
	CurrentAnnotator = geoloader.GetLatestData()
	currentDataMutex.Unlock()
}
