package metrics

import (
	"net/http"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//These vars are the prometheus metrics
var (
	ActiveRequests = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_Running_Annotation_Requests_Count",
		Help: "The current number of unfulfilled annotation service requests.",
	})
	RequestTimes = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "annotator_Request_Response_Time_Summary",
		Help: "The response time of each request, in nanoseconds.",
	})
	TotalRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "annotator_Annotation_Requests_total",
		Help: "The total number of annotation service requests.",
	})
	TotalLookups = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "annotator_Annotation_Lookups_total",
		Help: "The total number of ip lookups.",
	})
)

func init() {
	prometheus.MustRegister(ActiveRequests)
	prometheus.MustRegister(TotalRequests)
	prometheus.MustRegister(TotalLookups)
	prometheus.MustRegister(RequestTimes)
}

// SetupPrometheus sets up and runs a webserver to export prometheus metrics.
func SetupPrometheus() *http.Server {
	// Define a custom serve mux for prometheus to listen on a separate port.
	// We listen on a separate port so we can forward this port on the host VM.
	// We cannot forward port 8080 because it is used by AppEngine.
	mux := http.NewServeMux()
	// Assign the default prometheus handler to the standard exporter path.
	mux.Handle("/metrics", promhttp.Handler())
	// Assign the pprof handling paths to the external port to access individual
	// instances.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := &http.Server{
		Addr:    ":9090",
		Handler: mux,
	}
	go server.ListenAndServe()
	return server
}
