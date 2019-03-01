package geoloader

import (
	"fmt"
	"log"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
)

var (
	asnRegexV4 = regexp.MustCompile(`RouteViewIPv4/\d{4}/\d{2}/routeviews-(oix|rv2)-\d{8}-\d{4}\.pfx2as\.gz`) // matches to the IPv4 RouteView datasets
	asnRegexV6 = regexp.MustCompile(`RouteViewIPv6/\d{4}/\d{2}/routeviews-rv6-\d{8}-\d{4}\.pfx2as\.gz`)       // matches to the IPv6 RouteView datasets
)

// Helper function for unit tests to narrow the datasets to load from GCS to a specific date.
// The parameters are int pointers. If a parameter is nil, no filter will be used for that date part.
func UseSpecificASNDate(year, month, day *int) {
	yearStr := `\d{4}`
	monthStr := `\d{2}`
	dayStr := monthStr

	if year != nil {
		yearStr = fmt.Sprintf("%04d", *year)
	}
	if month != nil {
		monthStr = fmt.Sprintf("%02d", *month)
	}
	if day != nil {
		dayStr = fmt.Sprintf("%02d", *day)
	}

	asnRegexV4 = regexp.MustCompile(fmt.Sprintf(`RouteViewIPv4/%s/%s/routeviews-(oix|rv2)-%s%s%s-\d{4}\.pfx2as\.gz`, yearStr, monthStr, yearStr, monthStr, dayStr))
	asnRegexV6 = regexp.MustCompile(fmt.Sprintf(`RouteViewIPv6/%s/%s/routeviews-rv6-%s%s%s-\d{4}\.pfx2as\.gz`, yearStr, monthStr, yearStr, monthStr, dayStr))
	log.Printf("Date filter is set to %s%s%s", yearStr, monthStr, dayStr)
}

// ASNv4Loader should be used to load ASNv4 RouteView files
func ASNv4Loader(
	loader func(*storage.ObjectAttrs) (api.Annotator, error)) api.CachingLoader {
	return newCachingLoader(
		func(file *storage.ObjectAttrs) error {
			return filter(file, asnRegexV4, time.Time{})
		},
		loader,
		api.RouteViewPrefix)
}

// ASNv4Loader should be used to load ASNv6 RouteView files
func ASNv6Loader(
	loader func(*storage.ObjectAttrs) (api.Annotator, error)) api.CachingLoader {
	return newCachingLoader(
		func(file *storage.ObjectAttrs) error {
			return filter(file, asnRegexV6, time.Time{})
		},
		loader,
		api.RouteViewPrefix)
}
