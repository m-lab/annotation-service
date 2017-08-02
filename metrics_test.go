package annotator

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Our implementation of the gaugeMock object
type gaugeMock struct {
	i *bool
	d *bool
}

func (c gaugeMock) Inc() {
	if *c.d {
		*c.i = false
	} else {
		*c.i = true
	}
}

func (c gaugeMock) Dec() {
	if *c.i {
		*c.d = true
	} else {
		*c.d = false
	}
}

func (c gaugeMock) Add(_ float64)                      {}
func (c gaugeMock) Describe(_ chan<- *prometheus.Desc) {}
func (c gaugeMock) Collect(_ chan<- prometheus.Metric) {}
func (c gaugeMock) Desc() *prometheus.Desc             { return nil }
func (c gaugeMock) Set(_ float64)                      {}
func (c gaugeMock) SetToCurrentTime()                  {}
func (c gaugeMock) Sub(_ float64)                      {}
func (c gaugeMock) Write(_ *dto.Metric) error          { return nil }

//Our implementation of the summaryMock object
type summaryMock struct {
	observeCount *int
}

func (s summaryMock) Observe(_ float64) {
	*s.observeCount++
}

func (c summaryMock) Collect(_ chan<- prometheus.Metric) {}
func (c summaryMock) Desc() *prometheus.Desc             { return nil }
func (c summaryMock) Describe(_ chan<- *prometheus.Desc) {}
func (c summaryMock) Write(_ *dto.Metric) error          { return nil }
