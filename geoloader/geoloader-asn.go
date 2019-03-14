package geoloader

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/m-lab/annotation-service/asn"
	"github.com/m-lab/annotation-service/loader"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
)

const (
	// Folder prefix containing the RouteView files
	routeViewPrefix = "RouteView"
)

var (
	asnRegexV4 = regexp.MustCompile(`RouteViewIPv4/\d{4}/\d{2}/routeviews-(oix|rv2)-\d{6}01-\d{4}\.pfx2as\.gz`) // matches to the IPv4 RouteView datasets (first day of each month)
	asnRegexV6 = regexp.MustCompile(`RouteViewIPv6/\d{4}/\d{2}/routeviews-rv6-\d{6}01-\d{4}\.pfx2as\.gz`)       // matches to the IPv6 RouteView datasets (first day of each month)

	asnV4StartTime = time.Date(2009, time.Month(2), 1, 0, 0, 0, 0, time.UTC) // load V4 data from 2009. 02 (info from @yachang on slack)
	asnV6StartTime = time.Date(2018, time.Month(6), 1, 0, 0, 0, 0, time.UTC) // load V6 data from 2018. 06 (info from @yachang on slack)

	errNeededLoadingDate = errors.New("Befoore needed loading date")
)

// UseSpecificASNDateForTesting is for unit tests to narrow the datasets to load from GCS to date that can be matched to the date part regexes.
// The parameters are string pointers. If a parameter is nil, no filter will be used for that date part.
func UseSpecificASNDateForTesting(yearRegex, monthRegex, dayRegex *string) {
	asnV4StartTime = time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	asnV6StartTime = asnV4StartTime

	yearStr := `\d{4}`
	monthStr := `\d{2}`
	dayStr := monthStr

	if yearRegex != nil {
		yearStr = *yearRegex
	}
	if monthRegex != nil {
		monthStr = *monthRegex
	}
	if dayRegex != nil {
		dayStr = *dayRegex
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
			return asnFilterFrom(file, asnRegexV4, asnV4StartTime)
		},
		loader,
		routeViewPrefix)
}

// ASNv6Loader should be used to load ASNv6 RouteView files
func ASNv6Loader(
	loader func(*storage.ObjectAttrs) (api.Annotator, error)) api.CachingLoader {
	return newCachingLoader(
		func(file *storage.ObjectAttrs) error {
			return asnFilterFrom(file, asnRegexV6, asnV6StartTime)
		},
		loader,
		routeViewPrefix)
}

func asnFilterFrom(file *storage.ObjectAttrs, r *regexp.Regexp, from time.Time) error {
	baseFilename := loader.GetGzBase(file.Name)
	fileTime, err := asn.ExtractTimeFromASNFileName(baseFilename)
	if err != nil {
		return err
	}

	if from.After(*fileTime) {
		return errNeededLoadingDate
	}

	if !r.MatchString(file.Name) {
		return errNoMatch
	}

	return nil
}
