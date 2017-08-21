package handler

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/m-lab/annotation-service/metrics"
)

var currentDataMutex = &sync.RWMutex{}

func SetupHandlers() {
	http.HandleFunc("/annotate", Annotate)
	go waitForDownloaderMessages()
}

// Annotate looks up IP address and returns geodata.
func Annotate(w http.ResponseWriter, r *http.Request) {
	_, _, _, err := validate(w, r)
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

// validates request syntax
// parses request and returns parameters
// 0 for IPversion means that there was an error.
func validate(w http.ResponseWriter, r *http.Request) (IPversion int, s string, num time.Time, err error) {
	// Setup timers and counters for prometheus metrics.
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.Metrics_requestTimes.Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)

	metrics.Metrics_activeRequests.Inc()
	defer metrics.Metrics_activeRequests.Dec()

	query := r.URL.Query()

	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		return 0, s, num, errors.New("Invalid time")
	}

	ip := query.Get("ip_addr")

	newIP := net.ParseIP(ip)
	if newIP == nil {
		return 0, s, num, errors.New("Invalid IP address.")
	}
	if newIP.To4() != nil {
		return 4, ip, time.Unix(time_milli, 0), nil
	}
	return 6, ip, time.Unix(time_milli, 0), nil
}
