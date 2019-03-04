package asn_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geoloader"

	"github.com/m-lab/annotation-service/asn"
	"github.com/stretchr/testify/assert"
)

func TestAnnotateV4(t *testing.T) {
	datasetsTime := time.Date(2019, 1, 1, 12, 0, 0, 0, time.UTC)
	ann := getAnnotatorForDay(t, true, datasetsTime.Year(), int(datasetsTime.Month()), datasetsTime.Day(), datasetsTime)

	// test simple ASN
	geoData := api.GeoData{}
	err := ann.Annotate("84.1.28.246", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		[]api.ASNElement{
			api.ASNElement{[]string{"5483"}, api.ASNSingle},
		},
		geoData.ASN)

	// test set ASN
	geoData.ASN = nil
	err = ann.Annotate("43.224.118.23", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		[]api.ASNElement{
			api.ASNElement{[]string{"135001"}, api.ASNSingle},
			api.ASNElement{[]string{"135527"}, api.ASNSingle},
		},
		geoData.ASN)

	// test multi-origin ASN
	geoData.ASN = nil
	err = ann.Annotate("43.228.124.11", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		[]api.ASNElement{
			api.ASNElement{[]string{"133322", "133905"}, api.ASNMultiOrigin},
		},
		geoData.ASN)

	// test already populated error
	err = ann.Annotate("43.228.11", &geoData)
	assert.EqualError(t, err, "ErrAlreadyPopulated")

	// test bad IP error
	geoData.ASN = nil
	err = ann.Annotate("43.228.11", &geoData)
	assert.EqualError(t, err, "ErrInvalidIP")
}

func TestAnnotateV6(t *testing.T) {
	datasetsTime := time.Date(2019, 1, 1, 12, 0, 0, 0, time.UTC)
	ann := getAnnotatorForDay(t, false, datasetsTime.Year(), int(datasetsTime.Month()), datasetsTime.Day(), datasetsTime)

	// test simple ASN
	geoData := api.GeoData{}
	err := ann.Annotate("2001:408:8000:0000:0000:0000:0000:1313", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		[]api.ASNElement{
			api.ASNElement{[]string{"14793"}, api.ASNSingle},
		},
		geoData.ASN)

	// test set ASN
	// []string{"271", "7860", "8111"}
	geoData.ASN = nil
	err = ann.Annotate("2001:410:0000:0000:0000:0000:0000:1313", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		[]api.ASNElement{
			api.ASNElement{[]string{"271"}, api.ASNSingle},
			api.ASNElement{[]string{"7860"}, api.ASNSingle},
			api.ASNElement{[]string{"8111"}, api.ASNSingle},
		},
		geoData.ASN)

	// test multi-origin ASN
	geoData.ASN = nil
	err = ann.Annotate("2001:2b8:90:0000:0000:0000:0000:1313", &geoData)
	assert.Nil(t, err)
	assertASNData(t,
		[]api.ASNElement{
			api.ASNElement{[]string{"17832", "1237"}, api.ASNMultiOrigin},
		},
		geoData.ASN)

	// test already populated error
	err = ann.Annotate("2001:2b8:i3", &geoData)
	assert.EqualError(t, err, "ErrAlreadyPopulated")

	// test bad IP error
	geoData.ASN = nil
	err = ann.Annotate("2001:2b8:i3", &geoData)
	assert.EqualError(t, err, "ErrInvalidIP")
}

func TestExtractTimeFromASNFileName(t *testing.T) {
	// test success scenario
	successMap := map[string]time.Time{}
	successMap["routeviews-rv6-20070101-1309.pfx2as"] = time.Date(2007, 1, 1, 13, 9, 0, 0, time.UTC)
	successMap["routeviews-rv6-20190201-0930.pfx2as"] = time.Date(2019, 2, 1, 9, 30, 0, 0, time.UTC)

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

func bToMb(b uint64) uint64 {
	return b >> 20
}

func getAnnotatorForDay(t *testing.T, v4 bool, year, month, day int, datasetStartTime time.Time) api.Annotator {
	geoloader.UseSpecificASNDate(&year, &month, &day)

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

func assertASNData(t *testing.T, expectedASNs, gotASNs []api.ASNElement) {
	assert.Equal(t, len(expectedASNs), len(gotASNs))
	for idx, exp := range expectedASNs {
		got := gotASNs[idx]
		assert.Equal(t, exp.ASNList, got.ASNList)
		assert.Equal(t, exp.ASNListType, got.ASNListType)
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