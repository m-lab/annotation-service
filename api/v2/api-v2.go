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
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/metrics"
	"github.com/m-lab/go/logx"
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
	AnnotatorDate time.Time                   // The publication date(s) of the dataset used for the annotation
	Annotations   map[string]*api.Annotations // Map from human readable IP address to GeoData
}

// Annotator defines the GetAnnotations method used for annotating.
// info is an optional string to populate Request.RequestInfo
type Annotator interface {
	// TODO - make info an regular parameter instead of vararg.
	GetAnnotations(ctx context.Context, date time.Time, ips []string, info ...string) (*Response, error)
}

/*************************************************************************
*                           Remote Annotator API                          *
*************************************************************************/
var (
	RequestTimeHistogram = promauto.NewHistogramVec(
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

func post(ctx context.Context, url string, encodedData []byte) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(encodedData))
	if err != nil {
		log.Println(err)
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
			log.Println(err)
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
			log.Println(ctx.Err())
			RequestTimeHistogram.WithLabelValues("timeout").Observe(float64(time.Since(start).Nanoseconds()) / 1e6)
			return nil, ctx.Err()
		}
		// This is a recoverable error, so we should retry.
		RequestTimeHistogram.WithLabelValues("retry").Observe(float64(time.Since(start).Nanoseconds()) / 1e6)
		err = waitOneSecond(ctx)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
}

// ErrMoreJSON is returned if the message body was not completely consumed by decoder.
var ErrMoreJSON = errors.New("JSON body not completely consumed")

var decodeLogEvery = logx.NewLogEvery(nil, 30*time.Second)

// GetAnnotations takes a url, and Request, makes remote call, and returns parsed ResponseV2
// TODO make this unexported once we have migrated all code to use GetAnnotator()
func GetAnnotations(ctx context.Context, url string, date time.Time, ips []string, info ...string) (*Response, error) {
	req := NewRequest(date, ips)
	if len(info) > 0 {
		req.RequestInfo = info[0]
	}
	encodedData, err := json.Marshal(req)
	if err != nil {
		log.Println(err)
		metrics.ClientErrorTotal.WithLabelValues("request encoding error").Inc()
		return nil, err
	}

	httpResp, err := postWithRetry(ctx, url, encodedData)
	if err != nil {
		log.Println(err)
		if httpResp == nil || httpResp.Body == nil {
			metrics.ClientErrorTotal.WithLabelValues("http empty response").Inc()
			return nil, err
		}
		defer httpResp.Body.Close()
		if err == ErrStatusNotOK {
			metrics.ClientErrorTotal.WithLabelValues("http status not OK").Inc()
			body, ioutilErr := ioutil.ReadAll(httpResp.Body)
			if ioutilErr != nil {
				return nil, ioutilErr
			}
			// To avoid some bug causing a gigantic error string...
			if len(body) > 60 { // 60 is completely arbitrary.
				body = body[0:60]
			}
			return nil, fmt.Errorf("%s : %s", httpResp.Status, string(body))
		}
		return nil, err
	}

	defer httpResp.Body.Close()
	// Handle other status codes
	if httpResp.StatusCode != http.StatusOK {
		log.Println("http status not OK")
		metrics.ClientErrorTotal.WithLabelValues("http Status not OK").Inc()
		return nil, errors.New(httpResp.Status)
	}
	// Copy response into a byte slice
	body, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		log.Println(err)
		metrics.ClientErrorTotal.WithLabelValues("cannot read http response").Inc()
		return nil, err
	}

	resp := Response{}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()

	err = decoder.Decode(&resp)
	if err != nil {
		// TODO add metric, but in the correct namespace???
		// When this happens, it is likely to be very spammy.
		decodeLogEvery.Println("Decode error:", err)

		// Try again but ignore unknown fields.
		decoder := json.NewDecoder(bytes.NewReader(body))
		err = decoder.Decode(&resp)
		if err != nil {
			// This is a more serious error.
			log.Println(err)
			metrics.ClientErrorTotal.WithLabelValues("json decode error").Inc()
			return nil, err
		}
	}
	if decoder.More() {
		decodeLogEvery.Println("Decode error:", ErrMoreJSON)
	}
	return &resp, nil
}

type annotator struct {
	url string
}

func (ann annotator) GetAnnotations(ctx context.Context, date time.Time, ips []string, info ...string) (*Response, error) {
	return GetAnnotations(ctx, ann.url, date, ips, info...)
}

// GetAnnotator returns a v2.Annotator that uses the provided url to make v2 api requests.
func GetAnnotator(url string) Annotator {
	return &annotator{url: url}
}
