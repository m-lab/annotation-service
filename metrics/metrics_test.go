package metrics_test

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/m-lab/annotation-service/metrics"
	"github.com/prometheus/prometheus/util/promlint"
)

func TestPrometheusMetrics(t *testing.T) {
	server := metrics.SetupPrometheus()
	defer server.Shutdown(nil)

	metricReader, err := http.Get("http://localhost:9090/metrics")
	if err != nil || metricReader == nil {
		t.Errorf("Could not GET metrics: %v", err)
	}
	metricBytes, err := ioutil.ReadAll(metricReader.Body)
	if err != nil {
		t.Errorf("Could not read metrics: %v", err)
	}
	metricsLinter := promlint.New(bytes.NewBuffer(metricBytes))
	problems, err := metricsLinter.Lint()
	if err != nil {
		t.Errorf("Could not lint metrics: %v", err)
	}
	for _, p := range problems {
		t.Errorf("Bad metric %v: %v", p.Metric, p.Text)
	}
}
