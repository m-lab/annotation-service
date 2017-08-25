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
	"sync"
	"time"

	"github.com/m-lab/annotation-service/metrics"
	"github.com/m-lab/etl/schema"
)

// A mutex to make sure that we are not reading from the dataset while
// trying to update it
var currentDataMutex = &sync.RWMutex{}

// A function to set up any handlers that are needed, including url
// handlers and pubsub handlers
func SetupHandlers() {
	http.HandleFunc("/annotate", Annotate)
	http.HandleFunc("/batch_annotate", BatchAnnotate)
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
	defer metrics.Metrics_activeRequests.Dec()

	data, err := ValidateAndParse(r)
	if err != nil {
		fmt.Fprintf(w, "Invalid request")
		return
	}

	result := GetMetadataForSingleIP(data)
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
func ValidateAndParse(r *http.Request) (*schema.RequestData, error) {
	query := r.URL.Query()

	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		return nil, errors.New("Invalid time")
	}

	ip := query.Get("ip_addr")

	newIP := net.ParseIP(ip)
	if newIP == nil {
		return nil, errors.New("Invalid IP address")
	}
	if newIP.To4() != nil {
		return &schema.RequestData{ip, 4, time.Unix(time_milli, 0)}, nil
	}
	return &schema.RequestData{ip, 6, time.Unix(time_milli, 0)}, nil
}

// BatchAnnotate is a URL handler that expects the body of the request
// to contain a JSON encoded slice of schema.RequestDatas. It will
// look up all the ip addresses and bundle them into a map of metadata
// structs (with the keys being the ip concatenated with the base 36
// encoded timestamp) and send them back, again JSON encoded.
func BatchAnnotate(w http.ResponseWriter, r *http.Request) {
	// Setup timers and counters for prometheus metrics.
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.Metrics_requestTimes.Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)

	dataSlice, err := BatchValidateAndParse(r.Body)
	r.Body.Close()

	if err != nil {
		fmt.Fprintf(w, "Invalid Request!")
		return
	}

	responseMap := make(map[string]*schema.MetaData)
	for _, data := range dataSlice {
		responseMap[data.IP+strconv.FormatInt(data.Timestamp.Unix(), 36)] = GetMetadataForSingleIP(&data)
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
// schema.RequestDatas. It will then validate that json and use it to
// construct a slice of schema.RequestDatas, which it will return. If
// it encounters an error, then it will return nil and that error.
func BatchValidateAndParse(source io.Reader) ([]schema.RequestData, error) {
	jsonBuffer, err := ioutil.ReadAll(source)
	validatedData := []schema.RequestData{}
	if err != nil {
		return nil, err
	}
	uncheckedData := []struct {
		IP      string
		Unix_ts int64
	}{}

	err = json.Unmarshal(jsonBuffer, &uncheckedData)
	if err != nil {
		return nil, err
	}
	for _, data := range uncheckedData {
		newIP := net.ParseIP(data.IP)
		if newIP == nil {
			return nil, errors.New("Invalid IP address.")
		}
		ipType := 6
		if newIP.To4() != nil {
			ipType = 4
		}
		validatedData = append(validatedData, schema.RequestData{data.IP, ipType, time.Unix(data.Unix_ts, 0)})
	}
	return validatedData, nil
}

// GetMetadataForSingleIP takes a pointer to a schema.RequestData
// struct and will use it to fetch the appropriate associated
// metadata, returning a pointer. It is gaurenteed to return a non-nil
// pointer, even if it cannot find the appropriate metadata.
func GetMetadataForSingleIP(request *schema.RequestData) *schema.MetaData {
	// TODO: Figure out which table to use
	// TODO: Handle request
	currentDataMutex.RLock()
	defer currentDataMutex.RUnlock()
	// Fake response
	return &schema.MetaData{Geo: &schema.GeolocationIP{City: "Not A Real City", Postal_code: "10583"}, ASN: &schema.IPASNData{}}

}
