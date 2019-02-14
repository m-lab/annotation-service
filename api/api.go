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
// geolocation data that will be inserted into big query. The fiels are
// capitalized for exporting, although the originals in the DB schema
// are not.
// This is in common because it is used by the etl repository.
type GeolocationIP struct {
	ContinentCode string  `json:"continent_code,,omitempty" bigquery:"continent_code"` // Gives a shorthand for the continent
	CountryCode   string  `json:"country_code,,omitempty"   bigquery:"country_code"`   // Gives a shorthand for the country
	CountryCode3  string  `json:"country_code3,,omitempty"  bigquery:"country_code3"`  // Gives a shorthand for the country
	CountryName   string  `json:"country_name,,omitempty"   bigquery:"country_name"`   // Name of the country
	Region        string  `json:"region,,omitempty"         bigquery:"region"`         // Region or State within the country
	MetroCode     int64   `json:"metro_code,,omitempty"     bigquery:"metro_code"`     // Metro code within the country
	City          string  `json:"city,,omitempty"           bigquery:"city"`           // City within the region
	AreaCode      int64   `json:"area_code,,omitempty"      bigquery:"area_code"`      // Area code, similar to metro code
	PostalCode    string  `json:"postal_code,,omitempty"    bigquery:"postal_code"`    // Postal code, again similar to metro
	Latitude      float64 `json:"latitude,,omitempty"       bigquery:"latitude"`       // Latitude
	Longitude     float64 `json:"longitude,,omitempty"      bigquery:"longitude"`      // Longitude
}

// ASNData is the struct that will hold the IP/ASN data when it gets added to the
// schema. Currently empty and unused.
type ASNData struct{}

// GeoData is the main struct for the geo metadata, which holds pointers to the
// Geolocation data and the IP/ASN data. This is what we parse the JSON
// response from the annotator into.
// TODO - replace this with type Annotations struct.
type GeoData struct {
	Geo *GeolocationIP // Holds the geolocation data
	ASN *ASNData       // Holds the ASN data
}

/*************************************************************************
*                       Request/Response Structs                         *
*************************************************************************/

// The RequestData schema is the schema for the json that we will send
// down the pipe to the annotation service.
// DEPRECATED
// Should instead use a single Date (time.Time) and array of net.IP.
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
	Annotate(ip string, ann *GeoData) error

	// The date associated with the dataset.
	AnnotatorDate() time.Time

	// AnnotatorType returns a string indicating what kind of annotator this is.
	// AnnotatorType() string

	// Free any unsafe memory associated with the annotator.
	Close()
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

type Loader interface {
	// Update takes a map of annotators, and updates it to add any new annotators that have become
	// available.
	Update() error

	// Fetch returns the current list of annotators.
	Fetch() []Annotator
}
