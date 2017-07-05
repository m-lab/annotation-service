package annotator

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const appkey = "Temp Key"

var match_ip *regexp.Regexp

//These vars are the prometheus metrics
var (
	metrics_activeRequests = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_Running_Annotation_Requests_Count",
		Help: "The current number of unfulfilled annotation service requests.",
	})
	metrics_requestTimes = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "annotator_Request_Response_Time_Summary",
		Help: "The response time of each request, in nanoseconds.",
	})
)

func setupPrometheus() {
	http.Handle("/metrics", promhttp.Handler())
	prometheus.MustRegister(metrics_activeRequests)
	prometheus.MustRegister(metrics_requestTimes)
}

func init() {
	match_ip, _ = regexp.Compile(`^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4})$`)
	http.HandleFunc("/", handler)
	http.HandleFunc("/search_location", search_location)
	http.HandleFunc("/annotate", annotate)
	setupPrometheus()
}

func lookupAndRespond(w http.ResponseWriter, ip string, time_milli int64) {
	fmt.Fprintf(w, "I got ip %s and time since epoch %d.", ip, time_milli)
}

func annotate(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics_activeRequests.Inc()
	defer metrics_activeRequests.Dec()
	defer metrics_requestTimes.Observe(float64(time.Since(start).Nanoseconds()))
	query := r.URL.Query()
	ip := query.Get("ip_addr")
	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		fmt.Fprint(w, "INVALID TIME!")
		return
	}
	if !match_ip.MatchString(ip) {
		fmt.Fprint(w, "NOT A RECOGNIZED IP FORMAT!")
		return
	}
	lookupAndRespond(w, ip, time_milli)
}

func search_location(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength == 0 {
		fmt.Fprint(w, "EMPTY BODY!")
		return
	}

	body_buffer := make([]byte, r.ContentLength)
	_, err := io.ReadFull(r.Body, body_buffer)

	if err != nil {
		fmt.Fprint(w, "ERROR READING BODY")
		return
	}

	var location_request interface{}
	err = json.Unmarshal(body_buffer, &location_request)

	if err != nil {
		fmt.Fprint(w, "CANNOT PARSE REQUEST")
		return
	}
	loc_map := location_request.(map[string]interface{}) // Patch generic interface to a map of JSON key/value pairs
	fmt.Fprint(w, loc_map["IP_Addr"])
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Hello, world!")
}
