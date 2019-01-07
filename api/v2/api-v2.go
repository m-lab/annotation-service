package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

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
*                           Remote Annotator API                          *
*************************************************************************/
var (
	RequestTimeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "annotator_external_latency_hist_msec",
			Help: "annotator latency distributions.",
			Buckets: []float64{
				1.0, 1.3, 1.6, 2.0, 2.5, 3.2, 4.0, 5.0, 6.3, 7.9,
				10, 13, 16, 20, 25, 32, 40, 50, 63, 79,
				100, 130, 160, 200, 250, 320, 400, 500, 630, 790,
				1000, 1300, 1600, 2000, 2500, 3200, 4000, 5000, 6300, 7900,
			},
		},
		[]string{"detail"})
)

func init() {
	prometheus.MustRegister(RequestTimeHistogram)
}

func post(ctx context.Context, url string, encodedData []byte) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(encodedData))
	if err != nil {
		return nil, err
	}

	// Make the actual request
	return http.DefaultClient.Do(httpReq.WithContext(ctx))
}

// ErrStatusNotOK is returned from GetAnnotation if http status is other than OK.  Response body may have more info.
var ErrStatusNotOK = errors.New("http status not StatusOK")

func waitOneSecond(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Second):
		return nil
	}
}

// postWithRetry will retry for some error conditions, up to the deadline in the provided context.
// Returns if http status is OK, error is not nil, http status is not ServiceUnavailable or timeout.
func postWithRetry(ctx context.Context, url string, encodedData []byte) (*http.Response, error) {
	for {
		start := time.Now()
		resp, err := post(ctx, url, encodedData)
		if err != nil {
			RequestTimeHistogram.WithLabelValues(err.Error()).Observe(float64(time.Since(start).Nanoseconds()) / 1e6)
			return nil, err
		}
		if resp.StatusCode == http.StatusOK {
			RequestTimeHistogram.WithLabelValues("success").Observe(float64(time.Since(start).Nanoseconds()) / 1e6)
			return resp, err
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			RequestTimeHistogram.WithLabelValues(resp.Status).Observe(float64(time.Since(start).Nanoseconds()) / 1e6)
			return resp, ErrStatusNotOK
		}
		if ctx.Err() != nil {
			RequestTimeHistogram.WithLabelValues("timeout").Observe(float64(time.Since(start).Nanoseconds()) / 1e6)
			return nil, ctx.Err()
		}
		// This is a recoverable error, so we should retry.
		RequestTimeHistogram.WithLabelValues("retry").Observe(float64(time.Since(start).Nanoseconds()) / 1e6)
		err = waitOneSecond(ctx)
		if err != nil {
			return nil, err
		}
	}
}

// GetAnnotations takes a url, and Request, makes remote call, and returns parsed ResponseV2
// TODO(gfr) Should pass the annotator's request context through and use it here.
func GetAnnotations(ctx context.Context, url string, date time.Time, ips []string) (*Response, error) {
	req := NewRequest(date, ips)
	encodedData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpResp, err := postWithRetry(ctx, url, encodedData)
	if err != nil {
		if httpResp == nil || httpResp.Body == nil {
			return nil, err
		}
		defer httpResp.Body.Close()
		if err == ErrStatusNotOK {
			body, ioutilErr := ioutil.ReadAll(httpResp.Body)
			if ioutilErr != nil {
				return nil, ioutilErr
			}
			// To avoid some bug causing a gigantic error string...
			if len(body) > 60 { // 60 is completely arbitrary.
				body = body[0:60]
			}
			// URGENT TODO This is producing too many unique error types, spamming Prometheus!!
			// Started Jan 7, 16:27 UTC
			// This will require a rebuild of ETL clients.
			log.Printf("%s : %s\n", httpResp.Status, string(body))
			if len(httpResp.Status) > 30 {
				return nil, fmt.Errorf("%d %s...%s", httpResp.StatusCode, httpResp.Status[:15], httpResp.Status[len(httpResp.Status)-15:])
			}
			return nil, fmt.Errorf("%s", httpResp.Status)
		}
		return nil, err
	}

	defer httpResp.Body.Close()
	// Handle other status codes
	if httpResp.StatusCode != http.StatusOK {
		return nil, errors.New(httpResp.Status)
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
