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

func lookupAndRespond(w http.ResponseWriter, ip string, time_milli int64) {
	fmt.Fprintf(w, "I got ip %s and time since epoch %d.", ip, time_milli)
}

func annotate(w http.ResponseWriter, r *http.Request) {
	// Setup timers and counters for prometheus metrics.
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics_requestTimes.Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)

	metrics_activeRequests.Inc()
	defer metrics_activeRequests.Dec()

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

	lookupAndRespond(w, ip, time_milli)
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Place requests to /annotate with URL parameters ip_addr and since_epoch!")
}
