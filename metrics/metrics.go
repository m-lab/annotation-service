package metrics

import (
	"net/http"

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
)

func SetupPrometheus() {
	http.Handle("/metrics", promhttp.Handler())
	prometheus.MustRegister(Metrics_activeRequests)
	prometheus.MustRegister(Metrics_requestTimes)
}
