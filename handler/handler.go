package handler

import (
	"errors"
	"fmt"
	"io"
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

	_, err := ValidateAndParse(r)
	if err != nil {
		fmt.Fprintf(w, "Invalid request")
	} else {
		// Fake response
		currentDataMutex.RLock()
		defer currentDataMutex.RUnlock()
		fmt.Fprintf(w, `{"Geo":{"city": "%s", "postal_code":"10583"},"ASN":{}}`, "Not A Real City")
		// TODO: Figure out which table to use
		// TODO: Handle request
	}
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

func BatchAnnotate(w http.ResponseWriter, r *http.Request) {

}

func BatchValidateAndParse(source io.Reader) ([]schema.RequestData, error) {
	return nil, nil
}

func GetMetadataForSingleIP(request *schema.RequestData) *schema.MetaData {
	return nil
}
