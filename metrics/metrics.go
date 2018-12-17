package metrics

import (
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/m-lab/go/rtx"
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
	BadIPTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "annotator_Bad_IP_Addresses_total",
		Help: "The total number of ip parse failures.",
	})
	ErrorTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "annotator_Error_total",
		Help: "The total number annotation errors.",
	}, []string{"type"})

	DatasetCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_dataset_count",
		Help: "Number of datasets loaded in cache.",
	})

	PendingLoads = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_pending_load_count",
		Help: "Number of datasets currently being loaded.",
	})

	EvictionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "annotator_evictions_total",
		Help: "The total number datasets evicted.",
	})
	LoadCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "annotator_loads_total",
		Help: "The total number of datasets loaded.",
	})

	RejectionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "annotator_rejections_total",
		Help: "The total number of rejected requests.",
	}, []string{"type"})
)

func init() {
	prometheus.MustRegister(ActiveRequests)
	prometheus.MustRegister(TotalRequests)
	prometheus.MustRegister(TotalLookups)
	prometheus.MustRegister(RequestTimes)
	prometheus.MustRegister(BadIPTotal)
	prometheus.MustRegister(ErrorTotal)

	prometheus.MustRegister(DatasetCount)
	prometheus.MustRegister(PendingLoads)
	prometheus.MustRegister(EvictionCount)
	prometheus.MustRegister(LoadCount)
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

	// Don't ListenAndServe because we want to be able to GET as soon as this function returns.
	// Listen synchronously.
	listener, err := net.Listen("tcp", server.Addr)
	rtx.Must(err, "Could not open listening socket for Prometheus metrics")
	// Serve asynchronously.
	go server.Serve(listener.(*net.TCPListener))

	return server
}
