// Package api contains interfaces and low level structs required across
// multiple packages or repositories.
package api

import (
	"encoding/json"
	"os"
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
	ContinentCode string  `json:"continent_code,,omitempty"` // Gives a shorthand for the continent
	CountryCode   string  `json:"country_code,,omitempty"`   // Gives a shorthand for the country
	CountryCode3  string  `json:"country_code3,,omitempty"`  // Gives a shorthand for the country
	CountryName   string  `json:"country_name,,omitempty"`   // Name of the country
	Region        string  `json:"region,,omitempty"`         // Region or State within the country
	MetroCode     int64   `json:"metro_code,,omitempty"`     // Metro code within the country
	City          string  `json:"city,,omitempty"`           // City within the region
	AreaCode      int64   `json:"area_code,,omitempty"`      // Area code, similar to metro code
	PostalCode    string  `json:"postal_code,,omitempty"`    // Postal code, again similar to metro
	Latitude      float64 `json:"latitude,,omitempty"`       // Latitude
	Longitude     float64 `json:"longitude,,omitempty"`      // Longitude
}

// IPASNData is the struct that will hold the IP/ASN data when it gets added to the
// schema. Currently empty and unused.
type IPASNData struct{}

// GeoData is the main struct for the geo metadata, which holds pointers to the
// Geolocation data and the IP/ASN data. This is what we parse the JSON
// response from the annotator into.
type GeoData struct {
	Geo *GeolocationIP // Holds the geolocation data
	ASN *IPASNData     // Holds the IP/ASN data
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

// Annotator provides the GetAnnotation method, which retrieves the annotation for a given IP address.
type Annotator interface {
	// TODO use simple string IP
	GetAnnotation(request *RequestData) (GeoData, error)
	// The date associated with the dataset.
	AnnotatorDate() time.Time
}
