package metrics_test

import (
	"testing"

	"github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/annotation-service/metrics"
	"github.com/m-lab/go/prometheusx"
)

func TestPrometheusMetrics(t *testing.T) {
	api.RequestTimeHistogram.WithLabelValues("x")
	metrics.RequestTimeHistogramUsec.WithLabelValues("x", "x")
	metrics.ErrorTotal.WithLabelValues("x")
	metrics.RejectionCount.WithLabelValues("x")
	prometheusx.LintMetrics(t)
}
