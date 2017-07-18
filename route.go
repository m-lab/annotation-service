package annotator

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

var ipRegexp *regexp.Regexp

func init() {
	ipRegexp, _ = regexp.Compile(`^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4})$`)
	http.HandleFunc("/", handler)
	http.HandleFunc("/annotate", annotate)
	setupPrometheus()
}

func annotate(w http.ResponseWriter, r *http.Request) {
	// Setup timers and counters for prometheus metrics.
	timerStart := time.Now()
	defer metrics_requestTimes.Observe(float64(time.Since(timerStart).Nanoseconds()))

	metrics_activeRequests.Inc()
	defer metrics_activeRequests.Dec()

	time.Sleep(3)

	query := r.URL.Query()

	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		fmt.Fprint(w, "INVALID TIME!")
		return
	}

	ip := query.Get("ip_addr")
	if !ipRegexp.MatchString(ip) {
		fmt.Fprint(w, "NOT A RECOGNIZED IP FORMAT!")
		return
	}

	lookupAndRespond(r, w, ip, time_milli)
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Place requests to /annotate with URL parameters ip_addr and since_epoch!")
}
