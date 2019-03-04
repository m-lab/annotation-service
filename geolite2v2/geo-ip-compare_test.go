package geolite2v2_test

import (
	_ "net/http/pprof"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Pallinder/go-randomdata"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/geolite2v2"
	"github.com/m-lab/annotation-service/geoloader"
)

// TestCompareAnnotations tests if the new implementation annotates the same way as the old
// implementation
func TestCompareAnnotations(t *testing.T) {

	oldAnnotators := loadOld(t)
	newAnnotators := loadNew(t)

	// sort the annotators to be able to compare their resuults
	sort.Slice(oldAnnotators, createSorterFor(oldAnnotators))
	sort.Slice(newAnnotators, createSorterFor(newAnnotators))

	// we need minimum 10 000 IP hits per annotator
	minimumHitCountPerAnnotator := 10000

	// get each annotator
	for idx, oldAnn := range oldAnnotators {
		newAnn := newAnnotators[idx]
		notFoundCount := 0
		hitCount := 0
		ipV4 := true

		// annotate v4 and v6 IP addresses and compare the resuults
		for hitCount < minimumHitCountPerAnnotator {
			var oldResp, newResp api.GeoData
			var oldErr, newErr error

			var address string
			if ipV4 {
				address = randomdata.IpV4Address()
			} else {
				address = randomdata.IpV6Address()
			}
			ipV4 = !ipV4

			// the error should be the same if there's any
			oldErr = oldAnn.Annotate(address, &oldResp)
			newErr = newAnn.Annotate(address, &newResp)
			if oldErr != nil {
				notFoundCount++
				assert.EqualError(t, newErr, oldErr.Error())
				continue
			}
			// the content should be the same if there's any
			hitCount++
			assertSameGeoData(t, &oldResp, &newResp)
		}

		t.Logf("Not found count: %d, hit count: %d", notFoundCount, hitCount)
	}
}

func assertSameGeoData(t *testing.T, old, new *api.GeoData) {
	assert.Equal(t, old.Geo.AreaCode, new.Geo.AreaCode)
	assert.Equal(t, old.Geo.City, new.Geo.City)
	assert.Equal(t, old.Geo.ContinentCode, new.Geo.ContinentCode)
	assert.Equal(t, old.Geo.CountryCode, new.Geo.CountryCode)
	assert.Equal(t, old.Geo.CountryCode3, new.Geo.CountryCode3)
	assert.Equal(t, old.Geo.CountryName, new.Geo.CountryName)
	assert.Equal(t, old.Geo.Latitude, new.Geo.Latitude)
	assert.Equal(t, old.Geo.Longitude, new.Geo.Longitude)
	assert.Equal(t, old.Geo.MetroCode, new.Geo.MetroCode)
	assert.Equal(t, old.Geo.PostalCode, new.Geo.PostalCode)
	assert.Equal(t, old.Geo.Region, new.Geo.Region)
}

// TestCompareOldNewContent loads all the data with the old and the new implementation and check if contents are the same
func TestCompareOldNewContent(t *testing.T) {
	oldAnnotators := loadOld(t)
	newAnnotators := loadNew(t)

	// assert if we have the same number of annotators
	assert.Equal(t, len(oldAnnotators), len(newAnnotators))

	// sort the annotators to make sure we can compare the list items by index
	sort.Slice(oldAnnotators, createSorterFor(oldAnnotators))
	sort.Slice(newAnnotators, createSorterFor(newAnnotators))

	// assert full content
	for idx, oldAnn := range oldAnnotators {
		newAnn := newAnnotators[idx]
		oldDataset := oldAnn.(*geolite2.GeoDataset)
		newDataset := newAnn.(*geolite2v2.GeoDataset)
		assertSameDataset(t, oldDataset, newDataset)
	}
}

// assertSameDataset asserts if the annotators datasets are the same
func assertSameDataset(t *testing.T, oldDataset *geolite2.GeoDataset, newDataset *geolite2v2.GeoDataset) {
	assert.NotNil(t, oldDataset)
	assert.NotNil(t, newDataset)

	assert.True(t, oldDataset.Start.Equal(newDataset.Start))
	assertSameLocations(t, oldDataset.LocationNodes, newDataset.LocationNodes)
	assertSameIPNodes(t, oldDataset.IP4Nodes, newDataset.IP4Nodes)
	assertSameIPNodes(t, oldDataset.IP6Nodes, newDataset.IP6Nodes)

	assert.Equal(t, len(oldDataset.IP4Nodes), len(newDataset.IP4Nodes))
	assert.Equal(t, len(oldDataset.IP6Nodes), len(newDataset.IP6Nodes))
}

// assertSameLocations asserts if LocationNodes are the same
func assertSameLocations(t *testing.T, oldLocationNodes []geolite2.LocationNode, newLocationNodes []geolite2v2.LocationNode) {
	assert.Equal(t, len(oldLocationNodes), len(newLocationNodes))
	for idx, oldVal := range oldLocationNodes {
		newVal := newLocationNodes[idx]
		assert.Equal(t, oldVal.CityName, newVal.CityName)
		assert.Equal(t, oldVal.ContinentCode, newVal.ContinentCode)
		assert.Equal(t, oldVal.CountryCode, newVal.CountryCode)
		assert.Equal(t, oldVal.CountryName, newVal.CountryName)
		assert.Equal(t, oldVal.GeonameID, newVal.GeonameID)
		assert.Equal(t, oldVal.MetroCode, newVal.MetroCode)
		assert.Equal(t, oldVal.RegionCode, newVal.RegionCode)
		assert.Equal(t, oldVal.RegionName, newVal.RegionName)
	}
}

// assertSameIPNodes asserts if IPNodes are the same
func assertSameIPNodes(t *testing.T, oldIPNodes []geolite2.IPNode, newIPNodes []geolite2v2.GeoIPNode) {
	assert.Equal(t, len(oldIPNodes), len(newIPNodes))
	for idx, oldVal := range oldIPNodes {
		newVal := newIPNodes[idx]
		assert.True(t, oldVal.IPAddressLow.Equal(newVal.IPAddressLow))
		assert.True(t, oldVal.IPAddressHigh.Equal(newVal.IPAddressHigh))
		assert.Equal(t, oldVal.Latitude, newVal.Latitude)
		assert.Equal(t, oldVal.Longitude, newVal.Longitude)
		assert.Equal(t, oldVal.PostalCode, newVal.PostalCode)
		assert.Equal(t, oldVal.LocationIndex, newVal.LocationIndex)
	}
}

// createSorterFor returns a sorter function for the specified annotator list
func createSorterFor(forList []api.Annotator) func(int, int) bool {
	return func(firstIdx, nextIdx int) bool {
		return forList[firstIdx].AnnotatorDate().Before(forList[nextIdx].AnnotatorDate())
	}
}

// loadOld loads only data from march with the old loader
func loadOld(t *testing.T) []api.Annotator {
	geoloader.UseOnlyMarchForTest()
	g2loader := geoloader.Geolite2Loader(geolite2.LoadGeolite2)
	err := g2loader.UpdateCache()
	assert.Nil(t, err)
	return g2loader.Fetch()
}

// loadNew loads only data from march with the new loader
func loadNew(t *testing.T) []api.Annotator {
	geoloader.UseOnlyMarchForTest()
	g2loader := geoloader.Geolite2Loader(geolite2v2.LoadG2)
	err := g2loader.UpdateCache()
	assert.Nil(t, err)
	return g2loader.Fetch()
}
