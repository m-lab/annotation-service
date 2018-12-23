// Package geoloader provides the interface between manager and dataset handling
// packages (geolite2 and legacy). manager only depends on geoloader and api.
// geoloader only depends on geolite2, legacy and api.
package geoloader

import (
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/legacy"
)

func ArchivedLoader(filename string) (api.Annotator, error) {
	if GeoLite2Regex.MatchString(filename) {
		return geolite2.LoadGeoLite2Dataset(filename, api.MaxmindBucketName)
	} else {
		return legacy.LoadBundleDataset(filename, api.MaxmindBucketName)
	}
}
