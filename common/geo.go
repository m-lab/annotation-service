package common

import (
	"time"
)

// The GeolocationIP struct contains all the information needed for the
// geolocation data that will be inserted into big query. The fiels are
// capitalized for exporting, although the originals in the DB schema
// are not.
type GeolocationIP struct {
	Continent_code string  `json:"continent_code,,omitempty"` // Gives a shorthand for the continent
	Country_code   string  `json:"country_code,,omitempty"`   // Gives a shorthand for the country
	Country_code3  string  `json:"country_code3,,omitempty"`  // Gives a shorthand for the country
	Country_name   string  `json:"country_name,,omitempty"`   // Name of the country
	Region         string  `json:"region,,omitempty"`         // Region or State within the country
	Metro_code     int64   `json:"metro_code,,omitempty"`     // Metro code within the country
	City           string  `json:"city,,omitempty"`           // City within the region
	Area_code      int64   `json:"area_code,,omitempty"`      // Area code, similar to metro code
	Postal_code    string  `json:"postal_code,,omitempty"`    // Postal code, again similar to metro
	Latitude       float64 `json:"latitude,,omitempty"`       // Latitude
	Longitude      float64 `json:"longitude,,omitempty"`      // Longitude
}

// The struct that will hold the IP/ASN data when it gets added to the
// schema. Currently empty and unused.
type IPASNData struct{}

// The main struct for the geo metadata, which holds pointers to the
// Geolocation data and the IP/ASN data. This is what we parse the JSON
// response from the annotator into.
type GeoData struct {
	Geo *GeolocationIP // Holds the geolocation data
	ASN *IPASNData     // Holds the IP/ASN data
}

// The RequestData schema is the schema for the json that we will send
// down the pipe to the annotation service.
type RequestData struct {
	IP        string    // Holds the IP from an incoming request
	IPFormat  int       // Holds the ip format, 4 or 6
	Timestamp time.Time // Holds the timestamp from an incoming request
}
