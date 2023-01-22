// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The main package for the Prometheus server executable.
package main

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/prometheus/prometheus/promql"
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	promEngine "prometheus-singlestore-adapter/pkg/engine"
	"prometheus-singlestore-adapter/pkg/log"
	"prometheus-singlestore-adapter/pkg/reader"
	"prometheus-singlestore-adapter/pkg/util"

	"github.com/prometheus/prometheus/model/timestamp"

	s2prometheus "prometheus-singlestore-adapter/pkg/singlestore"

	"github.com/NYTimes/gziphandler"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jamiealquiza/envy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

type config struct {
	remoteTimeout      time.Duration
	listenAddr         string
	telemetryPath      string
	s2PrometheusConfig s2prometheus.Config
	logLevel           string
}

const (
	tickInterval      = time.Second
	promLivenessCheck = time.Second
)

var (
	receivedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	sentSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sent_samples_total",
			Help: "Total number of processed samples sent to remote storage.",
		},
		[]string{"remote"},
	)
	failedSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "failed_samples_total",
			Help: "Total number of processed samples which failed on send to remote storage.",
		},
		[]string{"remote"},
	)
	sentBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sent_batch_duration_seconds",
			Help:    "Duration of sample batch send calls to the remote storage.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"remote"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_ms",
			Help:    "Duration of HTTP request in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path"},
	)
	writeThroughput     = util.NewThroughputCalc(tickInterval)
	lastRequestUnixNano = time.Now().UnixNano()

	minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

func init() {
	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(sentSamples)
	prometheus.MustRegister(failedSamples)
	prometheus.MustRegister(sentBatchDuration)
	prometheus.MustRegister(httpRequestDuration)
	writeThroughput.Start()
}

func main() {
	cfg := parseFlags()
	log.Init(cfg.logLevel)
	log.Info("config", fmt.Sprintf("%+v", cfg))

	http.Handle(cfg.telemetryPath, promhttp.Handler())

	writer, readerClient := buildClients(cfg)

	http.Handle("/write", timeHandler("write", write(writer)))
	http.Handle("/read", timeHandler("read", read(readerClient)))
	http.Handle("/healthz", health(readerClient))

	engineOpts := promEngine.EngineOpts{
		Logger:                   log.GetLogger(),
		Reg:                      prometheus.NewRegistry(),
		MaxSamples:               5000000,
		Timeout:                  time.Minute * 2,
		LookbackDelta:            time.Minute * 5,
		NoStepSubqueryIntervalFn: func(int64) int64 { return durationMilliseconds(time.Minute) },
	}

	engine := promEngine.NewEngine(engineOpts)
	querier := s2prometheus.NewQuerier(
		timestamp.FromTime(time.Now().Add(-time.Hour*600)),
		timestamp.FromTime(time.Now().Add(time.Hour*600)),
		readerClient,
		labels.Labels{labels.Label{
			Name:  "handler",
			Value: "/",
		}},
		[]*labels.Matcher{},
	)
	http.HandleFunc("/api/v1/query", Query(queryZip(engine, querier)))

	log.Info("msg", "Starting up...")
	log.Info("msg", "Listening", "addr", cfg.listenAddr)

	err := http.ListenAndServe(cfg.listenAddr, nil)
	if err != nil {
		log.Error("msg", "Listen failure", "err", err)
		os.Exit(1)
	}
}

func Query(handler http.Handler) http.HandlerFunc {
	return func(responseWriter http.ResponseWriter, request *http.Request) {
		handler.ServeHTTP(responseWriter, request)
	}
}

func queryZip(engine *promEngine.Engine, querier s2prometheus.Querier) http.Handler {
	hf := queryHandler(engine, querier)
	return gziphandler.GzipHandler(hf)
}

func queryHandler(engine *promEngine.Engine, querier s2prometheus.Querier) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		if query == "" {
			http.Error(w, "query parameter is required", http.StatusBadRequest)
			return
		}

		var ts time.Time
		var err error
		ts, err = parseTimeParam(r, "time", time.Now())
		if err != nil {
			log.Error("msg", "Query error", "reason", err.Error())
			http.Error(w, "query parameter is required", http.StatusBadRequest)
			return
		}

		//ctx := r.Context()
		//if to := r.FormValue("timeout"); to != "" {
		//	var cancel context.CancelFunc
		//	timeout, err := parseDuration(to)
		//	if err != nil {
		//		log.Error("msg", "Query error", "err", err.Error())
		//		http.Error(w, "query parameter is required", http.StatusBadRequest)
		//		return
		//	}
		//
		//	ctx, cancel = context.WithTimeout(ctx, timeout)
		//	defer cancel()
		//}

		fmt.Printf("[Promql] ts: %s, query: %s\n", ts, query)

		expr, err := engine.NewInstantQuery(querier, &promEngine.QueryOpts{}, query, ts)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error parsing query: %s", err), http.StatusBadRequest)
			return
		}

		fmt.Printf("[Promql] expr: %s\n", expr.String())

		res := expr.Exec(r.Context())
		fmt.Printf("[Promql] result: %v, warnings: %v, err: %v \n", res.Value, res.Warnings, res.Err)
		val, er := res.Vector()
		fmt.Printf("[Promql] vector val: %v, err: %v\n", val, er)
		fmt.Printf("[Promql] result query string: %s\n", res.String())
		if res.Err != nil {
			log.Error("msg", res.Err, "endpoint", "query")
			switch res.Err.(type) {
			case promql.ErrQueryCanceled:
				respondError(w, http.StatusServiceUnavailable, res.Err, "canceled")
				return
			case promql.ErrQueryTimeout:
				respondError(w, http.StatusServiceUnavailable, res.Err, "timeout")
				return
			case promql.ErrStorage:
				respondError(w, http.StatusInternalServerError, res.Err, "internal")
				return
			}
			respondError(w, http.StatusUnprocessableEntity, res.Err, "execution")
			return
		}
		respondQuery(w, res, res.Warnings)
	}
}

func respondError(w http.ResponseWriter, status int, err error, errType string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	b, err := json.Marshal(&errResponse{
		Status:    "error",
		ErrorType: errType,
		Error:     err.Error(),
	})
	if err != nil {
		log.Error("msg", "error marshalling json error", "err", err)
	}
	if n, err := w.Write(b); err != nil {
		log.Error("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func respondQuery(w http.ResponseWriter, res *promql.Result, warnings storage.Warnings) {
	setResponseHeaders(w, res, false, warnings)
	switch resVal := res.Value.(type) {
	case promql.Vector:
		warnings := make([]string, 0, len(res.Warnings))
		for _, warn := range res.Warnings {
			warnings = append(warnings, warn.Error())
		}
		_ = marshalVectorResponse(w, resVal, warnings)
	case promql.Matrix:
		warnings := make([]string, 0, len(res.Warnings))
		for _, warn := range res.Warnings {
			warnings = append(warnings, warn.Error())
		}
		_ = marshalMatrixResponse(w, resVal, warnings)
	default:
		resp := &response{
			Status: "success",
			Data: &queryData{
				ResultType: res.Value.Type(),
				Result:     res.Value,
			},
		}
		for _, warn := range res.Warnings {
			resp.Warnings = append(resp.Warnings, warn.Error())
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, fmt.Errorf("Invalid time value for '%s': %w", paramName, err)
	}
	return result, nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseFlags() *config {
	cfg := &config{}

	s2prometheus.ParseFlags(&cfg.s2PrometheusConfig)

	flag.DurationVar(&cfg.remoteTimeout, "adapter-send-timeout", 30*time.Second, "The timeout to use when sending samples to the remote storage.")
	flag.StringVar(&cfg.listenAddr, "web-listen-address", ":9201", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.telemetryPath, "web-telemetry-path", "/metrics", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.logLevel, "log-level", "debug", "The log level to use [ \"error\", \"warn\", \"info\", \"debug\" ].")

	envy.Parse("TS_PROM")
	flag.Parse()

	return cfg
}

type writer interface {
	Write(samples model.Samples) error
	Name() string
}

type noOpWriter struct{}

func (no *noOpWriter) Write(samples model.Samples) error {
	log.Debug("msg", "Noop writer", "num_samples", len(samples))
	return nil
}

func (no *noOpWriter) Name() string {
	return "noopWriter"
}

func buildClients(cfg *config) (writer, reader.Reader) {
	s2Client := s2prometheus.NewClient(&cfg.s2PrometheusConfig)
	if s2Client.ReadOnly() {
		return &noOpWriter{}, s2Client
	}
	return s2Client, s2Client
}

func write(writer writer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		return
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Error("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Error("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		samples := protoToSamples(&req)
		receivedSamples.Add(float64(len(samples)))

		err = sendSamples(writer, samples)
		if err != nil {
			log.Warn("msg", "Error sending samples to remote storage", "err", err, "storage", writer.Name(), "num_samples", len(samples))
		}

		counter, err := sentSamples.GetMetricWithLabelValues(writer.Name())
		if err != nil {
			log.Warn("msg", "Couldn't get a counter", "labelValue", writer.Name(), "err", err)
		}
		writeThroughput.SetCurrent(getCounterValue(counter))

		select {
		case d := <-writeThroughput.Values:
			log.Debug("msg", "Samples write throughput", "samples/sec", d)
		default:
		}
	})
}

func getCounterValue(counter prometheus.Counter) float64 {
	dtoMetric := &io_prometheus_client.Metric{}
	if err := counter.Write(dtoMetric); err != nil {
		log.Warn("msg", "Error reading counter value", "err", err, "sentSamples", sentSamples)
	}
	return dtoMetric.GetCounter().GetValue()
}

func read(reader reader.Reader) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		return
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Error("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Error("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// log.Debug("-------READ QUERY-----")
		var resp *prompb.ReadResponse
		resp, err = reader.Read(&req)
		// log.Debug("--------------------")
		if err != nil {
			log.Warn("msg", "Error executing query", "query", req, "storage", reader.Name(), "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func health(reader reader.Reader) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := reader.HealthCheck()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "0")
	})
}

func protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}
	return samples
}

func sendSamples(w writer, samples model.Samples) error {
	atomic.StoreInt64(&lastRequestUnixNano, time.Now().UnixNano())
	begin := time.Now()
	err := w.Write(samples)
	duration := time.Since(begin).Seconds()
	if err != nil {
		failedSamples.WithLabelValues(w.Name()).Add(float64(len(samples)))
		return err
	}
	sentSamples.WithLabelValues(w.Name()).Add(float64(len(samples)))
	sentBatchDuration.WithLabelValues(w.Name()).Observe(duration)
	return nil
}

// timeHandler uses Prometheus histogram to track request time
func timeHandler(path string, handler http.Handler) http.Handler {
	f := func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		handler.ServeHTTP(w, r)
		elapsedMs := time.Since(start).Nanoseconds() / int64(time.Millisecond)
		httpRequestDuration.WithLabelValues(path).Observe(float64(elapsedMs))
	}
	return http.HandlerFunc(f)
}
