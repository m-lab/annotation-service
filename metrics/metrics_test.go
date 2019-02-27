package metrics_test

import (
	"testing"

	"github.com/m-lab/go/prometheusx"
)

func TestPrometheusMetrics(t *testing.T) {
	prometheusx.LintMetrics(t)
}
