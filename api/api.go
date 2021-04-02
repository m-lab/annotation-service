// Package api contains interfaces and low level structs required across
// multiple packages or repositories.
package api

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"time"
)

const (
	// Folder containing the maxmind files
	MaxmindPrefix = "Maxmind/"
)

var (
	// MaxmindBucketName is the bucket containing maxmind files.
	MaxmindBucketName = "downloader-" + os.Getenv("GCLOUD_PROJECT")
)

/*************************************************************************
*                             Annotation Structs                         *
*************************************************************************/

// The GeolocationIP struct contains all the information needed for the
// geolocation data that will be inserted into big query. The fields are
// capitalized for exporting, although the originals in the DB schema
// are not.
// This is in common because it is used by the etl repository.
type GeolocationIP struct {
	ContinentCode       string  `json:"continent_code,,omitempty" bigquery:"continent_code"` // Gives a shorthand for the continent
	CountryCode         string  `json:"country_code,,omitempty"   bigquery:"country_code"`   // Gives a shorthand for the country
	CountryCode3        string  `json:"country_code3,,omitempty"  bigquery:"country_code3"`  // Gives a shorthand for the country
	CountryName         string  `json:"country_name,,omitempty"   bigquery:"country_name"`   // Name of the country
	Region              string  `json:"region,,omitempty"         bigquery:"region"`         // Region or State within the country
	Subdivision1ISOCode string  `json:",omitempty"`                                          // ISO3166-2 first-level country subdivision ISO code
	Subdivision1Name    string  `json:",omitempty"`                                          // ISO3166-2 first-level country subdivision name
	Subdivision2ISOCode string  `json:",omitempty"`                                          // ISO3166-2 second-level country subdivision ISO code
	Subdivision2Name    string  `json:",omitempty"`                                          // ISO3166-2 second-level country subdivision name
	MetroCode           int64   `json:"metro_code,,omitempty"     bigquery:"metro_code"`     // Metro code within the country
	City                string  `json:"city,,omitempty"           bigquery:"city"`           // City within the region
	AreaCode            int64   `json:"area_code,,omitempty"      bigquery:"area_code"`      // Area code, similar to metro code
	PostalCode          string  `json:"postal_code,,omitempty"    bigquery:"postal_code"`    // Postal code, again similar to metro
	Latitude            float64 `json:"latitude,,omitempty"       bigquery:"latitude"`       // Latitude
	Longitude           float64 `json:"longitude,,omitempty"      bigquery:"longitude"`      // Longitude
	AccuracyRadiusKm    int64   `json:"radius,,omitempty"         bigquery:"radius"`         // Accuracy Radius (geolite2 from 2018)

	Missing bool `json:",omitempty"` // True when the Geolocation data is missing from MaxMind.
}

/************************************************************************
*                            ASN Annotations                            *
************************************************************************/

// We are currently using CAIDA Routeviews data to populate ASN annotations.
// See documentation at:
// http://data.caida.org/datasets/routing/routeviews-prefix2as/README.txt

// An AS mapping is either a single ASN or AS set, or a Multi-Origin AS, with 2 or more elements, each of which
// might be a single ASN or an AS set.
// The following scenarios are possible:
//
// Single ASN belongs to the IP:
//   - Example input: `"14061"`
//   - Example GeoData.ASData.Systems: `[{"ASNs": [14061]}]`
// An AS set for the IP:
//   - Example input: `"367,1479,1504"`
//   - Example GeoData.ASData.Systems: `[
//       {"ASNs": [367, 1479, 1504]}
//     ]`
// A multi-origin AS, consisting of multiple of AS sets:
//   - Example input: `"55967_38365_38365,64512,65323"`
//   - Example GeoData.Systems: `[
//       {"ASNs": [55967]},              // Appears most frequently
//       {"ASNs": [38365]},              // Appears less frequently
//       {"ASNs": [38365, 64512, 65323]} // Appears least frequently.
//     ]`
// Another multi-origin AS, consisting of multiple of AS sets:
//   - Example input: `"8508,199279_15744"`
//   - Example GeoData.Systems: `[
//       {"ASNs": [8508, 199279]},  // Appears most frequently
//       {"ASNs": [15744]},         // Appears less frequently
//     ]`

// A System is the base element.  It may contain a single ASN, or multiple ASNs comprising an AS set.
type System struct {
	// ASNs contains a single ASN, or AS set.  There must always be at least one ASN.
	// If there are more than one ASN, they are (arbitrarily) listed in increasing numerical order.
	ASNs []uint32
}

// ASData contains the Autonomous System information associated with the IP prefix.
// Roughly 99% of mappings consist of a single System with a single ASN.
//
// Looking at Routeviews data from 2019/01/01, the MOAS and AS set stats look like:
// IPv4:   Single: 99%    MOAS: 1%     AS set: .005% (of entries) 1/2 of AS sets start with MOAS
// IPv6:   Single: 99.3%  MOAS: 0.6%   AS set: .01%  (of entries) 1/3 of AS sets start with MOAS
// NOTE: This is NOT intended to be used directly as the BigQuery schema.
type ASData struct {
	IPPrefix string `json:",omitempty"` // the IP prefix found in the table.
	CIDR     string `json:",omitempty"` // The IP prefix found in the RouteViews data.
	ASNumber uint32 `json:",omitempty"` // First AS number.
	ASName   string `json:",omitempty"` // AS name for that number, data from IPinfo.io
	Missing  bool   `json:",omitempty"` // True when the ASN data is missing from RouteViews.

	// One or more "Systems".  There must always be at least one System.  If there are more than one,
	// then this is a Multi-Origin AS, and the component Systems are in order of frequency in routing tables,
	// most common first.
	Systems []System `json:",omitempty"`
}

// ErrNilOrEmptyASData is returned by BestASN if the ASData is nil or empty.
var ErrNilOrEmptyASData = errors.New("Empty or Nil ASData")

// BestASN returns a plausible ASN from a possibly complex ASData.
func (as *ASData) BestASN() (int64, error) {
	if as == nil || len(as.Systems) == 0 {
		return 0, ErrNilOrEmptyASData
	}
	sys0 := as.Systems[0]
	if len(sys0.ASNs) == 0 {
		return 0, ErrNilOrEmptyASData
	}
	return int64(sys0.ASNs[0]), nil
}

// GeoData is the main struct for the geo metadata, which holds pointers to the
// Geolocation data and the IP/ASN data. This is what we parse the JSON
// response from the annotator into.
// Deprecated: please use api.Annotations
type GeoData = Annotations

// Annotations is the main struct for annotation metadata, which holds pointers to the
// Geolocation data and the IP/ASN data. This is what we parse the JSON
// response from the annotator into.
type Annotations struct {
	Geo     *GeolocationIP // Holds the geolocation data
	Network *ASData        // Holds the associated network Autonomous System data.
}

/*************************************************************************
*                       Request/Response Structs                         *
*************************************************************************/

// The RequestData schema is the schema for the json that we will send
// down the pipe to the annotation service.
// DEPRECATED
// Should instead use a single Date (time.Time) and array of net.IP.  See the v2 API.
type RequestData struct {
	IP        string    // Holds the IP from an incoming request
	IPFormat  int       // Holds the ip format, 4 or 6
	Timestamp time.Time // Holds the timestamp from an incoming request
}

// RequestWrapper will be used for all future request types.
type RequestWrapper struct {
	RequestType string
	Body        json.RawMessage
}

/*************************************************************************
*                           Local Annotator API                          *
*************************************************************************/

// Annotator defines the methods required annotating
type Annotator interface {
	// Annotate populates one or more annotation fields in the GeoData object.
	// If it fails, it will return a non-nil error and will leave the target unmodified.
	Annotate(ip string, ann *Annotations) error

	// The date associated with the dataset.
	AnnotatorDate() time.Time
}

var dateRE = regexp.MustCompile(`[0-9]{8}T`)

// ExtractDateFromFilename return the date for a filename like
// gs://downloader-mlab-oti/Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz
// TODO: both geoloader and geolite2 package use this func, so leave it here for now.
func ExtractDateFromFilename(filename string) (time.Time, error) {
	filedate := dateRE.FindAllString(filename, -1)
	if len(filedate) != 1 {
		return time.Time{}, errors.New("cannot extract date from input filename")
	}
	return time.Parse(time.RFC3339, filedate[0][0:4]+"-"+filedate[0][4:6]+"-"+filedate[0][6:8]+"T00:00:00Z")
}

/*************************************************************************
*                            Annotator Loader                            *
*************************************************************************/

// CachingLoader keeps a cache of loaded annotators, updates the cache on request, and returns a copy
// of the cache on request.
type CachingLoader interface {
	// UpdateCache causes the loader to load any new annotators and add them to the cached list.
	UpdateCache() error

	// Fetch returns a copy of the current list of annotators.
	// May return an empty slice, but must not return nil.
	Fetch() []Annotator
}
