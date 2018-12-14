package metrics_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime/debug"
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

func rePanic() {
	defer func() {
		metrics.CountPanics(recover(), "foobar")
	}()
	a := []int{1, 2, 3}
	log.Println(a[4])
	// This is never reached.
	return
}

func TestCountPanics(t *testing.T) {
	// When we call RePanic, the panic should cause a log and a metric
	// increment, but should still panic.  This intercepts the panic,
	// and errors if the panic doesn't happen.
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
		fmt.Printf("%s\n", debug.Stack())
	}()

	rePanic()
}
