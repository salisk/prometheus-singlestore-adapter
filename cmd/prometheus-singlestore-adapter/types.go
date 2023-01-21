package main

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// ExemplarData is additional information associated with a time series.
type ExemplarData struct {
	Labels labels.Labels `json:"labels"`
	Value  float64       `json:"value"`
	Ts     int64         `json:"timestamp"` // This is int64 in Prometheus, but we do this to avoid later conversions to decimal.
}

type ExemplarQueryResult struct {
	SeriesLabels labels.Labels  `json:"seriesLabels"`
	Exemplars    []ExemplarData `json:"exemplars"`
}

type errResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Message   string `json:"message,omitempty"`
}

type response struct {
	Status   string      `json:"status"`
	Data     interface{} `json:"data,omitempty"`
	Warnings []string    `json:"warnings,omitempty"`
}

type queryData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
}
