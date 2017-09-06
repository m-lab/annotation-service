package metrics

import (
	"net/http"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//These vars are the prometheus metrics
var (
	Metrics_activeRequests = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_Running_Annotation_Requests_Count",
		Help: "The current number of unfulfilled annotation service requests.",
	})
	Metrics_requestTimes = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "annotator_Request_Response_Time_Summary",
		Help: "The response time of each request, in nanoseconds.",
	})
	Metrics_totalRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "annotator_Annotation_Requests_Total",
		Help: "The total number of annotation service requests.",
	})
	Metrics_totalLookups = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "annotator_Annotation_Lookups_Total",
		Help: "The total number of ip lookups.",
	})
)

func SetupPrometheus() {
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

	http.Handle("/metrics", promhttp.Handler())
	prometheus.MustRegister(Metrics_activeRequests)
	prometheus.MustRegister(Metrics_totalRequests)
	prometheus.MustRegister(Metrics_totalLookups)
	prometheus.MustRegister(Metrics_requestTimes)
	go http.ListenAndServe(":9090", mux)
}
