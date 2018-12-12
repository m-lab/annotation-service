// Package api contains interfaces and low level structs required across
// multiple packages or repositories.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"
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

// RequestV2Tag is the string associated with v2.0 requests.
const RequestV2Tag = "Annotate v2.0"

// RequestV2 describes the data we expect to receive (json encoded) in the request body.
type RequestV2 struct {
	RequestType string    // This should contain "Annotate v2.0"
	RequestInfo string    // Arbitrary info about the requester, to be used, e.g., for stats.
	Date        time.Time // The date to be used to annotate the addresses.
	IPs         []string  // The IP addresses to be annotated
}

// ResponseV2 describes data returned in V2 responses (json encoded).
type ResponseV2 struct {
	// TODO should we include additional metadata about the annotator sources?  Perhaps map of filenames?
	AnnotatorDate time.Time           // The publication date of the dataset used for the annotation
	Annotations   map[string]*GeoData // Map from human readable IP address to GeoData
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

// AnnotationLoader provides the Load function, which loads an annotator.
// TODO - do we really need this, or should we just have a single maxmind.Load function.
type AnnotationLoader interface {
	Load(date time.Time) (Annotator, error)
}

// ExtractDateFromFilename return the date for a filename like
// gs://downloader-mlab-oti/Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz
// TODO move this to maxmind package
// TODO - actually, this now seems to be dead code.  But probably needed again soon, so leaving it here.
func ExtractDateFromFilename(filename string) (time.Time, error) {
	re := regexp.MustCompile(`[0-9]{8}T`)
	filedate := re.FindAllString(filename, -1)
	if len(filedate) != 1 {
		return time.Time{}, errors.New("cannot extract date from input filename")
	}
	return time.Parse(time.RFC3339, filedate[0][0:4]+"-"+filedate[0][4:6]+"-"+filedate[0][6:8]+"T00:00:00Z")
}

// DoRPC takes a url, and RequestV2, makes remote call, and returns parsed ResponseV2
// TODO(gfr) Should pass the annotator's request context through and use it here.
func DoRPC(ctx context.Context, url string, req RequestV2) (*ResponseV2, error) {
	encodedData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	var netClient = &http.Client{
		// Median response time is < 10 msec, but 99th percentile is 0.6 seconds.
		Timeout: 2 * time.Second,
	}

	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(encodedData))
	if err != nil {
		return nil, err
	}

	// Make the actual request
	httpResp, err := netClient.Do(httpReq.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	// Catch errors reported by the service
	if httpResp.StatusCode != http.StatusOK {
		return nil, errors.New("URL:" + url + " gave response code " + httpResp.Status)
	}

	// Copy response into a byte slice
	body, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}

	resp := ResponseV2{}

	err = json.Unmarshal(body, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
