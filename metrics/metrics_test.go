package metrics_test

import (
	"testing"

	"github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/annotation-service/metrics"
	"github.com/m-lab/go/prometheusx/promtest"
)

// TestPrometheusMetrics ensures that all the metrics pass the linter. We apply
// labels to all metrics which require them in an effort to run all metrics
// through the linter.
func TestPrometheusMetrics(t *testing.T) {
	api.RequestTimeHistogram.WithLabelValues("x")
	metrics.RequestTimeHistogramUsec.WithLabelValues("x", "x")
	metrics.ErrorTotal.WithLabelValues("x")
	metrics.RejectionCount.WithLabelValues("x")
	promtest.LintMetrics(t)
}
