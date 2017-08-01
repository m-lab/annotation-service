package annotator

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

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
