package geolite2v2_test

import (
	"bytes"
	"log"
	_ "net/http/pprof"
	"sort"
	"testing"

	"github.com/Pallinder/go-randomdata"
	"github.com/stretchr/testify/assert"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/geolite2v2"
	"github.com/m-lab/annotation-service/geoloader"
)

// TestCompareAnnotations tests if the new implementation annotates the same way as the old
// implementation
func TestCompareAnnotations(t *testing.T) {
	if testing.Short() {
		log.Println("Skipping test that relies on mlab-testing bucket")
		return
	}
	oldAnnotators := loadOld(t)
	newAnnotators := loadNew(t)

	// sort the annotators to be able to compare their resuults
	sort.Slice(oldAnnotators, createSorterFor(oldAnnotators))
	sort.Slice(newAnnotators, createSorterFor(newAnnotators))

	// we need minimum 200 IP hits per annotator
	minimumHitCountPerAnnotator := 200

	// get each annotator
	for idx, oldAnn := range oldAnnotators {
		newAnn := newAnnotators[idx]
		notFoundCount, v4HitCount, v6HitCount := 0, 0, 0
		ipV4 := true

		// annotate v4 and v6 IP addresses and compare the resuults
		for (v4HitCount + v6HitCount) < minimumHitCountPerAnnotator {
			var oldResp, newResp api.GeoData
			var oldErr, newErr error

			var address string
			if ipV4 {
				address = randomdata.IpV4Address()
			} else {
				address = randomdata.IpV6Address()
			}

			// the error should be the same if there's any
			oldErr = oldAnn.Annotate(address, &oldResp)
			newErr = newAnn.Annotate(address, &newResp)
			if oldErr != nil {
				notFoundCount++
				assert.EqualError(t, newErr, oldErr.Error())
				continue
			}
			// the content should be the same if there's any
			assertSameGeoData(t, &oldResp, &newResp)

			if ipV4 {
				v4HitCount++
			} else {
				v6HitCount++
			}
			ipV4 = !ipV4

			if v4HitCount%100 == 0 || v6HitCount%100 == 0 {
				log.Printf("Not found count: %d, v4 hit count: %d, v6 hit count %d", notFoundCount, v4HitCount, v6HitCount)
			}
		}

		log.Printf("annotator[%d] - Not found count: %d, v4 hit count: %d, v6 hit count %d", idx, notFoundCount, v4HitCount, v6HitCount)
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
	if testing.Short() {
		log.Println("Skipping test that relies on mlab-testing bucket")
		return
	}
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

// assertSameIPNodes asserts if IPNodes are the same (note that the new version merges nodes if possible, so the comparison is a bit complex)
func assertSameIPNodes(t *testing.T, oldIPNodes []geolite2.IPNode, newIPNodes []geolite2v2.GeoIPNode) {
	oldIdx := 0
	// iterate over the new nodes
	for newIdx, newNode := range newIPNodes {
		oldNode := oldIPNodes[oldIdx]

		// in the beginning of every iteration the actual oldNode should be in the beginning of the newNode
		assert.True(t, newNode.IPAddressLow.Equal(oldNode.IPAddressLow))

		// we keep getting the next nodes until an oldNode reaches the end of the actual new node
		for ; bytes.Compare(oldNode.IPAddressHigh, newNode.IPAddressHigh) <= 0; oldNode = oldIPNodes[oldIdx] {
			// oldNode should be within the neNode
			assert.True(t, bytes.Compare(oldNode.IPAddressLow, newNode.IPAddressLow) >= 0)

			// all data should match
			assert.Equal(t, oldNode.Latitude, newNode.Latitude, "latitude at oldIdx=%d, newIdx=%d, oldNode=%v, newNode=%v", oldIdx, newIdx, oldNode, newNode)
			assert.Equal(t, oldNode.Longitude, newNode.Longitude, "longitude at oldIdx=%d, newIdx=%d, oldNode=%v, newNode=%v", oldIdx, newIdx, oldNode, newNode)
			assert.Equal(t, oldNode.PostalCode, newNode.PostalCode, "postal code at oldIdx=%d, newIdx=%d, oldNode=%v, newNode=%v", oldIdx, newIdx, oldNode, newNode)
			assert.Equal(t, oldNode.LocationIndex, newNode.LocationIndex, "locationindex at oldIdx=%d, newIdx=%d, oldNode=%v, newNode=%v", oldIdx, newIdx, oldNode, newNode)

			oldIdx++

			// if we reach the end of the old nodes double check that we!re in the end of the new nodes as well and exit
			if oldIdx == len(oldIPNodes) {
				assert.Equal(t, len(newIPNodes)-1, newIdx)
				assert.True(t, oldNode.IPAddressHigh.Equal(newNode.IPAddressHigh))
				break
			}
		}
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
	year, month, day := "2018", "03", "01"
	geoloader.UseSpecificGeolite2DateForTesting(&year, &month, &day)
	g2loader := geoloader.Geolite2Loader(geolite2.LoadGeolite2)
	err := g2loader.UpdateCache()
	assert.Nil(t, err)
	return g2loader.Fetch()
}

// loadNew loads only data from march with the new loader
func loadNew(t *testing.T) []api.Annotator {
	year, month, day := "2018", "03", "01"
	geoloader.UseSpecificGeolite2DateForTesting(&year, &month, &day)
	g2loader := geoloader.Geolite2Loader(geolite2v2.LoadG2)
	err := g2loader.UpdateCache()
	assert.Nil(t, err)
	return g2loader.Fetch()
}
