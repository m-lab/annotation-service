package metrics_test

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/metrics"
	"github.com/prometheus/prometheus/util/promlint"
)

func TestPrometheusMetrics(t *testing.T) {
	// server :=
	metrics.SetupPrometheus()
	// defer server.Shutdown(nil)  // This is causing crashes on Travis.

	metricReader, err := http.Get("http://localhost:9090/metrics")
	for err != nil && strings.Contains(err.Error(), "connection refused") {
		metricReader, err = http.Get("http://localhost:9090/metrics")
		time.Sleep(1 * time.Millisecond)
	}
	if err != nil || metricReader == nil {
		t.Fatalf("Could not GET metrics: %v", err)
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
