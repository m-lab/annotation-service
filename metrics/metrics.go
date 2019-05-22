package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//These vars are the prometheus metrics
var (
	// TODO make this an integer gauge
	ActiveRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_Running_Annotation_Requests_Count",
		Help: "The current number of unfulfilled annotation service requests.",
	})
	RequestTimes = promauto.NewSummary(prometheus.SummaryOpts{
		Name: "annotator_Request_Response_Time_Summary",
		Help: "The response time of each request, in nanoseconds.",
	})
	RequestTimeHistogramUsec = promauto.NewHistogramVec(
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
		[]string{"source", "type", "detail"})
	// Note the batch annotate request counted as 1 as well.
	TotalRequests = promauto.NewCounter(prometheus.CounterOpts{
		Name: "annotator_Annotation_Requests_total",
		Help: "The total number of annotation service requests.",
	})
	// Measure the number of IPs w/ missing anottaion fields. missing type
	// could be "geo", "asn", "both".
	ResponseMissingAnnotation = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "annotator_Annotation_Response_Missing_Annotation_total",
		Help: "The total number of annotation responses with missing annotation field.",
	}, []string{"missing_type"})

	TotalLookups = promauto.NewCounter(prometheus.CounterOpts{
		Name: "annotator_Annotation_Lookups_total",
		Help: "The total number of ip lookups.",
	})
	BadIPTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "annotator_Bad_IP_Addresses_total",
		Help: "The total number of ip parse failures.",
	}, []string{"type"})
	ErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "annotator_Error_total",
		Help: "The total number annotation errors.",
	}, []string{"type"})

	// TODO make this an integer gauge
	DatasetCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_num_datasets",
		Help: "Number of datasets loaded in cache.",
	})

	// TODO make this an integer gauge
	PendingLoads = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "annotator_num_pending_load",
		Help: "Number of datasets currently being loaded.",
	})

	EvictionCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "annotator_evictions_total",
		Help: "The total number datasets evicted.",
	})
	LoadCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "annotator_loads_total",
		Help: "The total number of datasets loaded.",
	})

	RejectionCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "annotator_rejections_total",
		Help: "The total number of rejected requests.",
	}, []string{"type"})
)
