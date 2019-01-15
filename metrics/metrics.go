package metrics

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/m-lab/go/rtx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//These vars are the prometheus metrics
var (
	// TODO make this an integer gauge
	ActiveRequests = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_Running_Annotation_Requests_Count",
		Help: "The current number of unfulfilled annotation service requests.",
	})
	RequestTimes = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "annotator_Request_Response_Time_Summary",
		Help: "The response time of each request, in nanoseconds.",
	})
	RequestTimeHistogramUsec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "annotator_latency_hist_usec",
			Help: "annotator latency distributions.",
			Buckets: []float64{
				10, 13, 16, 20, 25, 32, 40, 50, 63, 79,
				100, 130, 160, 200, 250, 320, 400, 500, 630, 790,
				1000, 1300, 1600, 2000, 2500, 3200, 4000, 5000, 6300, 7900,
				10000, 13000, 16000, 20000, 25000, 32000, 40000, 50000, 63000, 79000,
				100000, 130000, 160000, 200000, 250000, 320000, 400000, 500000, 630000, 790000,
				1000000, 1300000, 1600000, 2000000, 2500000, 3200000, 4000000, 5000000, 6300000, 7900000,
				10000000,
			},
		},
		[]string{"type", "detail"})
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

	// TODO make this an integer gauge
	DatasetCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_num_datasets",
		Help: "Number of datasets loaded in cache.",
	})

	// TODO make this an integer gauge
	PendingLoads = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_num_pending_load",
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
	prometheus.MustRegister(RejectionCount)
	prometheus.MustRegister(RequestTimeHistogramUsec)
}

// SetupPrometheus sets up and runs a webserver to export prometheus metrics.
func SetupPrometheus(port int) *http.Server {
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

	// TODO PBOOTHE - integrate common function into httpx.
	// Don't ListenAndServe because we want to be able to GET as soon as this function returns.
	// Listen synchronously.
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	rtx.Must(err, "Could not open listening socket for Prometheus metrics")
	port = listener.Addr().(*net.TCPAddr).Port

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	// Serve asynchronously.
	go server.Serve(listener.(*net.TCPListener))

	return server
}
