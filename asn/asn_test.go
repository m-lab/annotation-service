package asn_test

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/iputils"

	"github.com/go-test/deep"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geoloader"

	"github.com/m-lab/annotation-service/asn"
	"github.com/stretchr/testify/assert"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	// Set ipinfo CSV file path.
	asn.ASNamesFile = "testdata/asnames-test.csv"
}

func bToMb(b uint64) uint64 {
	return b >> 20
}

func getAnnotatorForDay(t *testing.T, v4 bool, datasetStartTime time.Time) api.Annotator {
	year := strconv.Itoa(datasetStartTime.Year())
	month := fmt.Sprintf("%02d", datasetStartTime.Month())
	// NOTE: ASN patterns are limited to the first day of the month.
	geoloader.UpdateASNDatePattern(year + "/" + month)

	var loader api.CachingLoader
	if v4 {
		loader = geoloader.ASNv4Loader(asn.LoadASNDataset)
	} else {
		loader = geoloader.ASNv6Loader(asn.LoadASNDataset)
	}

	err := loader.UpdateCache()
	assert.Nil(t, err)

	annotators := loader.Fetch()
	assert.Equal(t, 1, len(annotators))

	ann := annotators[0]
	assert.True(t, ann.AnnotatorDate().Equal(datasetStartTime))

	return ann
}

func assertASNData(t *testing.T, expected, got *api.ASData) {
	if !assert.Nil(t, deep.Equal(expected, got)) {
		t.Logf("%+v\n", got)
	}
}

func dumpMemoryStats(t *testing.T) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("Alloc = %v MiB", bToMb(m.Alloc))
	t.Logf("TotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	t.Logf("Sys = %v MiB", bToMb(m.Sys))
	t.Logf("NumGC = %v\n", m.NumGC)
}

func TestAnnotateV4(t *testing.T) {
	if testing.Short() {
		t.Skip("Ignoring test that depend on GCS")
	}
	datasetsTime := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	ann := getAnnotatorForDay(t, true, datasetsTime)

	// test simple ASN
	geoData := api.GeoData{}
	err := ann.Annotate("1.0.128.100", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		&api.ASData{
			Systems:  []api.System{api.System{ASNs: []uint32{23969}}},
			CIDR:     "1.0.128.0/23",
			ASNumber: 23969,
			ASName:   "TOT Public Company Limited",
		},
		geoData.Network)

	// test set ASN
	geoData.Network = nil
	err = ann.Annotate("37.203.240.10", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		&api.ASData{
			Systems:  []api.System{api.System{ASNs: []uint32{199430, 202079}}},
			CIDR:     "37.203.240.0/24",
			ASNumber: 199430,
			ASName:   "Limited Liability Company GOODWOOD",
		},
		geoData.Network)

	// test multi-origin ASN
	geoData.Network = nil
	err = ann.Annotate("37.142.80.10", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		&api.ASData{
			Systems: []api.System{
				api.System{ASNs: []uint32{12849}},
				api.System{ASNs: []uint32{65024}}},
			CIDR:     "37.142.80.0/21",
			ASNumber: 12849,
			ASName:   "Hot-Net internet services Ltd.",
		},
		geoData.Network)

	// test already populated error
	err = ann.Annotate("43.228.11", &geoData)
	assert.EqualError(t, err, "ErrAlreadyPopulated")

	// test bad IP error
	geoData.Network = nil
	err = ann.Annotate("43.228.11", &geoData)
	assert.EqualError(t, err, iputils.ErrInvalidIP.Error())
}

func TestAnnotateV6(t *testing.T) {
	if testing.Short() {
		t.Skip("Ignoring test that depend on GCS")
	}
	datasetsTime := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	ann := getAnnotatorForDay(t, false, datasetsTime)

	// test simple ASN
	geoData := api.GeoData{}
	err := ann.Annotate("2001:2b8:18:0000:0000:0000:0000:1313", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		&api.ASData{
			Systems:  []api.System{api.System{ASNs: []uint32{17832}}},
			CIDR:     "2001:2b8::/43",
			ASNumber: 17832,
			ASName:   "Korea Internet Security Agency",
		},
		geoData.Network)

	// test set ASN
	geoData.Network = nil
	err = ann.Annotate("2001:410:0000:0000:0000:0000:0000:1313", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		&api.ASData{
			Systems:  []api.System{api.System{ASNs: []uint32{271, 7860, 8111, 26677}}},
			CIDR:     "2001:410::/47",
			ASNumber: 271,
			ASName:   "BCnet",
		},
		geoData.Network)

	// test multi-origin ASN
	geoData.Network = nil
	err = ann.Annotate("2001:428:00:0000:0000:0000:0000:1313", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		&api.ASData{
			Systems: []api.System{
				{ASNs: []uint32{209}},
				{ASNs: []uint32{3910}},
				{ASNs: []uint32{3908}},
			},
			CIDR:     "2001:428::/39",
			ASNumber: 209,
			ASName:   "Qwest Communications Company, LLC",
		},
		geoData.Network)

	// test already populated error
	err = ann.Annotate("2001:2b8:i3", &geoData)
	assert.EqualError(t, err, "ErrAlreadyPopulated")

	// test bad IP error
	geoData.Network = nil
	err = ann.Annotate("2001:2b8:i3", &geoData)
	assert.EqualError(t, err, iputils.ErrInvalidIP.Error())
}

func TestExtractTimeFromASNFileName(t *testing.T) {
	// test success scenario
	successMap := map[string]time.Time{}
	successMap["routeviews-rv6-20070101-1309.pfx2as"] = time.Date(2007, 1, 1, 0, 0, 0, 0, time.UTC)
	successMap["routeviews-rv6-20190201-0930.pfx2as"] = time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	for file, time := range successMap {
		extractedTime, err := asn.ExtractTimeFromASNFileName(file)
		assert.Nil(t, err)
		assert.True(t, time.Equal(*extractedTime))
	}

	// test fail scenarios
	errStrings := []string{
		"routeviews-rv6-2000101-1309.pfx2as",
		"doggy",
	}

	for _, file := range errStrings {
		extractedTime, err := asn.ExtractTimeFromASNFileName(file)
		assert.Nil(t, extractedTime)
		assert.Error(t, err)
	}
}
