// Package handler provides functions for handling incoming requests.
// It should only include top level code for parsing the request and assembling
// the response.
package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/metrics"
)

const (
	// This is the base in which we should encode the timestamp when we
	// are creating the keys for the mapt to return for batch requests
	encodingBase = 36
)

// SetupHandlers sets up any handlers that are needed, including url
// handlers and pubsub handlers
func SetupHandlers() {
	http.HandleFunc("/annotate", Annotate)
	http.HandleFunc("/batch_annotate", BatchAnnotate)
	// This listens for pubsub messages about new downloader files, and loads them
	// when they become available.
	go waitForDownloaderMessages()
}

// Annotate is a URL handler that looks up IP address and puts
// metadata out to the response encoded in json format.
func Annotate(w http.ResponseWriter, r *http.Request) {
	// Setup timers and counters for prometheus metrics.
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.Metrics_requestTimes.Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)
	metrics.Metrics_activeRequests.Inc()
	metrics.Metrics_totalRequests.Inc()
	defer metrics.Metrics_activeRequests.Dec()

	data, err := ValidateAndParse(r)
	if err != nil {
		fmt.Fprintf(w, "Invalid request")
		return
	}

	result, err := GetMetadataForSingleIP(data)
	if err != nil {
		fmt.Fprintf(w, "Cannot get meta data")
		return
	}

	encodedResult, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(w, "Unknown JSON Encoding Error")
		return
	}
	fmt.Fprint(w, string(encodedResult))
}

// ValidateAndParse takes a request and validates the URL parameters,
// verifying that it has a valid ip address and time. Then, it uses
// that to construct a RequestData struct and returns the pointer.
func ValidateAndParse(r *http.Request) (*api.RequestData, error) {
	query := r.URL.Query()

	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		return nil, errors.New("invalid time")
	}

	ip := query.Get("ip_addr")

	newIP := net.ParseIP(ip)
	if newIP == nil {
		return nil, errors.New("invalid IP address")
	}
	if newIP.To4() != nil {
		return &api.RequestData{ip, 4, time.Unix(time_milli, 0)}, nil
	}
	return &api.RequestData{ip, 6, time.Unix(time_milli, 0)}, nil
}

// BatchResponse is the response type for batch requests.  It is converted to
// json for HTTP requests.
type BatchResponse struct {
	Version string
	Date    time.Time
	Results map[string]*api.GeoData
}

// NewBatchResponse returns a new response struct.
// Caller must properly initialize the version and date strings.
// TODO - pass in the data source and use to populate the version/date.
func NewBatchResponse(size int) *BatchResponse {
	responseMap := make(map[string]*api.GeoData, size)
	return &BatchResponse{"", time.Time{}, responseMap}
}

// TODO move to annotatormanager package soon.
var ErrNoAnnotator = errors.New("no Annotator found")

// AnnotateLegacy uses a single `date` to select an annotator, and uses that annotator to annotate all
// `ips`.  It uses the dates from the individual RequestData to form the keys for the result map.
// Return values include the StartDate associated with the Annotator that was used.
// TODO move to annotatormanager package soon.
// DEPRECATED: This will soon be replaced with Annotate(), that will use net.IP instead of RequestData.
func AnnotateLegacy(date time.Time, ips []api.RequestData) (map[string]*api.GeoData, time.Time, error) {
	responseMap := make(map[string]*api.GeoData)

	ann := geolite2.GetAnnotator(date)
	if ann == nil {
		// stop sending more request in the same batch because w/ high chance the dataset is not ready
		return nil, time.Time{}, ErrNoAnnotator
	}

	for i := range ips {
		request := ips[i]
		metrics.Metrics_totalLookups.Inc()
		annotation, err := ann.GetAnnotation(&request)
		if err != nil {
			// TODO need better error handling.
			continue
		}
		// This requires that the caller should ignore the dateString.
		// TODO - the unit tests do not catch this problem, so maybe it isn't a problem.
		dateString := strconv.FormatInt(request.Timestamp.Unix(), encodingBase)
		responseMap[request.IP+dateString] = annotation
	}
	// TODO use annotator's actual start date.
	return responseMap, time.Time{}, nil
}

// BatchAnnotate is a URL handler that expects the body of the request
// to contain a JSON encoded slice of api.RequestDatas. It will
// look up all the ip addresses and bundle them into a map of metadata
// structs (with the keys being the ip concatenated with the base 36
// encoded timestamp) and send them back, again JSON encoded.
// TODO update this comment when we switch to new API.
func BatchAnnotate(w http.ResponseWriter, r *http.Request) {
	// Setup timers and counters for prometheus metrics.
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.Metrics_requestTimes.Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)
	metrics.Metrics_activeRequests.Inc()
	metrics.Metrics_totalRequests.Inc()
	defer metrics.Metrics_activeRequests.Dec()

	dataSlice, err := BatchValidateAndParse(r.Body)
	r.Body.Close()

	if err != nil {
		fmt.Fprintf(w, "Invalid Request!")
		return
	}

	var responseMap map[string]*api.GeoData

	// For now, use the date of the first item.  In future the items will not have individual timestamps.
	if len(dataSlice) > 0 {
		// For old request format, we use the date of the first RequestData
		date := dataSlice[0].Timestamp
		responseMap, _, err = AnnotateLegacy(date, dataSlice)
		if err != nil {
			fmt.Fprintf(w, err.Error())
			return
		}
	} else {
		responseMap = make(map[string]*api.GeoData)
	}

	encodedResult, err := json.Marshal(responseMap)
	if err != nil {
		fmt.Fprintf(w, "Unknown JSON Encoding Error")
		return
	}
	fmt.Fprint(w, string(encodedResult))
}

// BatchValidateAndParse will take a reader (likely the body of a
// request) containing the JSON encoded array of
// api.RequestDatas. It will then validate that json and use it to
// construct a slice of api.RequestDatas, which it will return. If
// it encounters an error, then it will return nil and that error.
func BatchValidateAndParse(source io.Reader) ([]api.RequestData, error) {
	jsonBuffer, err := ioutil.ReadAll(source)
	if err != nil {
		return nil, err
	}
	uncheckedData := []api.RequestData{}

	err = json.Unmarshal(jsonBuffer, &uncheckedData)
	if err != nil {
		return nil, err
	}
	validatedData := make([]api.RequestData, 0, len(uncheckedData))
	for _, data := range uncheckedData {
		newIP := net.ParseIP(data.IP)
		if newIP == nil {
			// TODO - shouldn't bail out because of a single error.
			return nil, errors.New("invalid IP address")
		}
		ipType := 6
		if newIP.To4() != nil {
			ipType = 4
		}
		validatedData = append(validatedData, api.RequestData{data.IP, ipType, data.Timestamp})
	}
	return validatedData, nil
}

// GetMetadataForSingleIP takes a pointer to a api.RequestData
// struct and will use it to fetch the appropriate associated
// metadata, returning a pointer. It is gaurenteed to return a non-nil
// pointer, even if it cannot find the appropriate metadata.
func GetMetadataForSingleIP(request *api.RequestData) (*api.GeoData, error) {
	metrics.Metrics_totalLookups.Inc()
	// TODO replace with generic GetAnnotator, that respects time.
	ann := geolite2.GetAnnotator(request.Timestamp)
	if ann == nil {
		return nil, geolite2.ErrNilDataset
	}

	return ann.GetAnnotation(request)
}
