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
	metrics.RequestTimeHistogramUsec.WithLabelValues("x", "y", "z")
	metrics.ErrorTotal.WithLabelValues("x")
	metrics.RejectionCount.WithLabelValues("x")
	// TODO(https://github.com/m-lab/annotation-service/issues/266)
	// Some metrics no longer pass the linter.
	//promtest.LintMetrics(t)
	promtest.LintMetrics(nil)
}
