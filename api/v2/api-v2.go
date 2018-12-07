package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/m-lab/annotation-service/api"
)

/*************************************************************************
*                       Request/Response Structs                         *
*************************************************************************/

// RequestWrapper will be used for all future request types.
type RequestWrapper struct {
	RequestType string
	Body        json.RawMessage
}

// RequestTag is the string associated with v2.0 requests.
const RequestTag = "Annotate v2.0"

// Request describes the data we expect to receive (json encoded) in the request body.
type Request struct {
	RequestType string    // This should contain "Annotate v2.0"
	RequestInfo string    // Arbitrary info about the requester, to be used, e.g., for stats.
	Date        time.Time // The date to be used to annotate the addresses.
	IPs         []string  // The IP addresses to be annotated
}

// NewRequest returns a partially initialized requests.  Caller should fill in IPs.
func NewRequest(date time.Time, ips []string) Request {
	return Request{Date: date, RequestType: RequestTag, IPs: ips}
}

// Response describes data returned in V2 responses (json encoded).
type Response struct {
	// TODO should we include additional metadata about the annotator sources?  Perhaps map of filenames?
	AnnotatorDate time.Time               // The publication date of the dataset used for the annotation
	Annotations   map[string]*api.GeoData // Map from human readable IP address to GeoData
}

/*************************************************************************
*                           Local Annotator API                          *
*************************************************************************/

// GetAnnotations takes a url, and Request, makes remote call, and returns parsed ResponseV2
// TODO(gfr) Should pass the annotator's request context through and use it here.
func GetAnnotations(ctx context.Context, url string, date time.Time, ips []string) (*Response, error) {
	req := NewRequest(date, ips)
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

	resp := Response{}

	err = json.Unmarshal(body, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}