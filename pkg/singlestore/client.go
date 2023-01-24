package s2prometheus

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"prometheus-singlestore-adapter/pkg/log"
	"prometheus-singlestore-adapter/pkg/util"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

// Config for the database
type Config struct {
	host                      string
	port                      int
	user                      string
	password                  string
	database                  string
	table                     string
	maxOpenConns              int
	maxIdleConns              int
	s2PrometheusNormalize     bool
	s2PrometheusLogSamples    bool
	s2PrometheusChunkInterval time.Duration
	dbConnectRetries          int
	readOnly                  bool
}

// ParseFlags parses the configuration flags specific to PostgreSQL and TimescaleDB
func ParseFlags(cfg *Config) *Config {
	flag.StringVar(&cfg.host, "pg-host", "localhost", "The PostgreSQL host")
	flag.IntVar(&cfg.port, "pg-port", 5432, "The PostgreSQL port")
	flag.StringVar(&cfg.user, "pg-user", "postgres", "The PostgreSQL user")
	flag.StringVar(&cfg.password, "pg-password", "", "The PostgreSQL password")
	flag.StringVar(&cfg.database, "pg-database", "postgres", "The PostgreSQL database")
	flag.StringVar(&cfg.table, "pg-table", "metrics", "Override prefix for internal tables. It is also a view name used for querying")
	flag.IntVar(&cfg.maxOpenConns, "pg-max-open-conns", 50, "The max number of open connections to the database")
	flag.IntVar(&cfg.maxIdleConns, "pg-max-idle-conns", 10, "The max number of idle connections to the database")
	flag.BoolVar(&cfg.s2PrometheusNormalize, "pg-prometheus-normalized-schema", true, "Insert metric samples into normalized schema")
	flag.BoolVar(&cfg.s2PrometheusLogSamples, "pg-prometheus-log-samples", false, "Log raw samples to stdout")
	flag.DurationVar(&cfg.s2PrometheusChunkInterval, "pg-prometheus-chunk-interval", time.Hour*12, "The size of a time-partition chunk in TimescaleDB")
	flag.IntVar(&cfg.dbConnectRetries, "pg-db-connect-retries", 0, "How many times to retry connecting to the database")
	flag.BoolVar(&cfg.readOnly, "pg-read-only", false, "Read-only mode. Don't write to database. Useful when pointing adapter to read replica")
	return cfg
}

// Client sends Prometheus samples to MySQL
type Client struct {
	DB  *sql.DB
	cfg *Config
}

const (
	sqlCreateTmpTable = "CREATE TEMPORARY TABLE IF NOT EXISTS %s(time BIGINT, name TEXT NOT NULL, value DOUBLE, labels JSON)"
	sqlInsertLabels   = "INSERT INTO %s_labels (metric_name, labels) SELECT name, labels FROM %s"
	sqlInsertValues   = "INSERT INTO %s_values (time, value, labels_id) SELECT tmp.time, tmp.value, l.id FROM %s as tmp INNER JOIN %s_labels AS l on tmp.name=l.metric_name AND tmp.labels=l.labels"
)

var createTmpTableStmt *sql.Stmt

// NewClient creates a new MySQL client
func NewClient(cfg *Config) *Client {
	connParams := strings.Join([]string{
		// convert timestame and date to time.Time
		"parseTime=true",
		// don't use the binary protocol
		"interpolateParams=true",
		// set a sane connection timeout rather than the default infinity
		"timeout=10s",
	}, "&")

	connString := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?%s",
		cfg.user,
		cfg.password,
		fmt.Sprintf("%s:%d", cfg.host, cfg.port),
		cfg.database,
		connParams,
	)

	wrappedDb, err := util.RetryWithFixedDelay(uint(cfg.dbConnectRetries), time.Second, func() (interface{}, error) {
		return sql.Open("mysql", connString)
	})

	log.Info("msg", regexp.MustCompile("password='(.+?)'").ReplaceAllLiteralString(connString, "password='****'"))

	if err != nil {
		log.Error("err", err)
		os.Exit(1)
	}

	db := wrappedDb.(*sql.DB)

	db.SetMaxOpenConns(cfg.maxOpenConns)
	db.SetMaxIdleConns(cfg.maxIdleConns)

	client := &Client{
		DB:  db,
		cfg: cfg,
	}

	err = client.setupS2Prometheus()
	if err != nil {
		log.Error("err", "Error on setting prometheus for SingleStore", err)
		os.Exit(1)
	}

	return client
}

func Escape(sql string) string {
	dest := make([]byte, 0, 2*len(sql))
	var escape byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]

		escape = 0

		switch c {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
			break
		case '\n': /* Must be escaped for logs */
			escape = 'n'
			break
		case '\r':
			escape = 'r'
			break
		case '\\':
			escape = '\\'
			break
		case '\'':
			escape = '\''
			break
		case '"': /* Better safe than sorry */
			escape = '"'
			break
		case '\032': // 十进制26,八进制32,十六进制1a, /* This gives problems on Win32 */
			escape = 'Z'
		}

		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}

	return string(dest)
}

func (c *Client) setupS2Prometheus() error {
	tx, err := c.DB.Begin()
	defer tx.Rollback()

	if c.cfg.s2PrometheusNormalize {
		_, err = c.DB.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_values(time BIGINT, value DOUBLE, labels_id BIGINT)", c.cfg.table))
		if err != nil {
			return err
		}

		_, err = c.DB.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_labels(id BIGINT NOT NULL AUTO_INCREMENT, metric_name TEXT NOT NULL, labels JSON, primary key (id))", c.cfg.table))
		if err != nil {
			return err
		}

		_, err = c.DB.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", c.cfg.table))
		if err != nil {
			return err
		}

		_, err = c.DB.Exec(fmt.Sprintf("CREATE VIEW %s AS SELECT v.time, l.metric_name AS name, v.value, l.labels FROM %s_values AS v JOIN %s_labels AS l ON l.id = v.labels_id", c.cfg.table, c.cfg.table, c.cfg.table))
		if err != nil {
			return err
		}
	} else {
		_, err := c.DB.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_samples(time BIGINT, name TEXT NOT NULL, value DOUBLE, labels JSON)", c.cfg.table))
		if err != nil {
			return err
		}

		_, err = c.DB.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", c.cfg.table))
		if err != nil {
			return err
		}

		_, err = c.DB.Exec(fmt.Sprintf("CREATE VIEW %s AS SELECT time, name AS metric_name, value, labels FROM %s_samples", c.cfg.table, c.cfg.table))
		if err != nil {
			return err
		}
	}

	tx.Commit()
	return nil
}

func (c *Client) ReadOnly() bool {
	return c.cfg.readOnly
}

func metricString(m model.Metric) string {
	metricName, hasName := m[model.MetricNameLabel]
	numLabels := len(m) - 1
	if !hasName {
		numLabels = len(m)
	}
	labelStrings := make([]string, 0, numLabels)
	for label, value := range m {
		if label != model.MetricNameLabel {
			labelStrings = append(labelStrings, fmt.Sprintf("%s=%q", label, value))
		}
	}

	switch numLabels {
	case 0:
		if hasName {
			return string(metricName)
		}
		return "{}"
	default:
		sort.Strings(labelStrings)
		return fmt.Sprintf("%s{%s}", metricName, strings.Join(labelStrings, ","))
	}
}

func metricLabelStrings(m model.Metric) (string, string) {
	metricName, hasName := m[model.MetricNameLabel]
	numLabels := len(m) - 1
	if !hasName {
		numLabels = len(m)
	}
	labelStrings := make([]string, 0, numLabels)
	for label, value := range m {
		if label != model.MetricNameLabel {
			labelStrings = append(labelStrings, fmt.Sprintf("%q:%q", label, value))
		}
	}

	switch numLabels {
	case 0:
		if hasName {
			return string(metricName), ""
		}
		return "", ""
	default:
		sort.Strings(labelStrings)
		return string(metricName), fmt.Sprintf("{%s}", strings.Join(labelStrings, ","))
	}
}

// Write implements the Writer interface and writes metric samples to the database
func (c *Client) Write(samples model.Samples) error {
	begin := time.Now()
	tx, err := c.DB.Begin()
	if err != nil {
		log.Error("msg", "Error on Begin when writing samples", "err", err)
		return err
	}

	defer tx.Rollback()

	var copyTable string
	if c.cfg.s2PrometheusNormalize {
		copyTable = fmt.Sprintf("%s_tmp", c.cfg.table)

		_, err = tx.Exec(fmt.Sprintf(sqlCreateTmpTable, copyTable))
		if err != nil {
			log.Error("msg", "Error executing create tmp table", "err", err)
			return err
		}
	} else {
		copyTable = fmt.Sprintf("%s_samples", c.cfg.table)
	}

	for _, sample := range samples {
		milliseconds := sample.Timestamp.UnixNano() / 1000000
		metricName, labels := metricLabelStrings(sample.Metric)
		line := fmt.Sprintf("%v%v %v %v\n", metricName, labels, sample.Value, milliseconds)

		if c.cfg.s2PrometheusLogSamples {
			fmt.Println(line)
		}

		var metricValue string
		if math.IsNaN(float64(sample.Value)) {
			metricValue = "NULL"
		} else if math.IsInf(float64(sample.Value), 1) {
			metricValue = fmt.Sprintf("%f", math.MaxFloat64)
		} else if math.IsInf(float64(sample.Value), -1) {
			metricValue = fmt.Sprintf("%f", -math.MaxFloat64)
		} else {
			metricValue = fmt.Sprintf("%f", sample.Value)
		}
		q := fmt.Sprintf("INSERT INTO %s VALUES(%v, \"%s\", %s, \"%s\")", copyTable, milliseconds, metricName, metricValue, Escape(labels))

		/* 		fmt.Println("------QUERY-----")
		   		fmt.Println(q)
		   		fmt.Println("----------------") */

		_, err = tx.Exec(q)
		if err != nil {
			log.Error("msg", "Error executing INSERT metrics statement", "stmt", line, "err", err)
			return err
		}
	}

	if copyTable == fmt.Sprintf("%s_tmp", c.cfg.table) {
		stmtLabels, err := tx.Prepare(fmt.Sprintf(sqlInsertLabels, c.cfg.table, copyTable))
		if err != nil {
			log.Error("msg", "Error on preparing labels statement", "err", err)
			return err
		}
		_, err = stmtLabels.Exec()
		if err != nil {
			log.Error("msg", "Error executing labels statement", "err", err)
			return err
		}

		stmtValues, err := tx.Prepare(fmt.Sprintf(sqlInsertValues, c.cfg.table, copyTable, c.cfg.table))
		if err != nil {
			log.Error("msg", "Error on preparing values statement", "err", err)
			return err
		}
		_, err = stmtValues.Exec()
		if err != nil {
			log.Error("msg", "Error executing values statement", "err", err)
			return err
		}

		err = stmtLabels.Close()
		if err != nil {
			log.Error("msg", "Error on closing labels statement", "err", err)
			return err
		}

		err = stmtValues.Close()
		if err != nil {
			log.Error("msg", "Error on closing values statement", "err", err)
			return err
		}

		_, err = tx.Exec(fmt.Sprintf("DROP TABLE %s", copyTable))
		if err != nil {
			log.Error("msg", "Error on dropping tmp table", "err", err)
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Error("msg", "Error on Commit when writing samples", "err", err)
		return err
	}

	duration := time.Since(begin).Seconds()
	log.Debug("msg", "Wrote samples", "count", len(samples), "duration", duration)
	return nil
}

type sampleLabels struct {
	JSON        []byte
	Map         map[string]string
	OrderedKeys []string
}

func createOrderedKeys(m *map[string]string) []string {
	keys := make([]string, 0, len(*m))
	for k := range *m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (c *Client) Close() {
	if c.DB != nil {
		if err := c.DB.Close(); err != nil {
			log.Error("msg", err.Error())
		}
	}
}

func (l *sampleLabels) Scan(value interface{}) error {
	if value == nil {
		l = &sampleLabels{}
		return nil
	}

	switch t := value.(type) {
	case []uint8:
		m := make(map[string]string)
		err := json.Unmarshal(t, &m)
		if err != nil {
			return err
		}

		*l = sampleLabels{
			JSON:        t,
			Map:         m,
			OrderedKeys: createOrderedKeys(&m),
		}
		return nil
	}
	return fmt.Errorf("invalid labels value %s", reflect.TypeOf(value))
}

func (l sampleLabels) String() string {
	return string(l.JSON)
}

func (l sampleLabels) key(extra string) string {
	// 0xff cannot cannot occur in valid UTF-8 sequences, so use it
	// as a separator here.
	separator := "\xff"
	pairs := make([]string, 0, len(l.Map)+1)
	pairs = append(pairs, extra+separator)

	for _, k := range l.OrderedKeys {
		pairs = append(pairs, k+separator+l.Map[k])
	}
	return strings.Join(pairs, separator)
}

func (l *sampleLabels) len() int {
	return len(l.OrderedKeys)
}

// Read implements the Reader interface and reads metrics samples from the database
func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	labelsToSeries := map[string]*prompb.TimeSeries{}

	for _, q := range req.Queries {
		command, err := c.buildCommand(q)
		if err != nil {
			return nil, err
		}

		log.Debug("msg", "Execute READ query", "query", command)

		rows, err := c.DB.Query(command)
		//fmt.Println("------command---------")
		//fmt.Println(command)
		//fmt.Println("------error-------")
		//fmt.Println(err)
		//fmt.Println("---end of command---------")
		if err != nil {
			return nil, err
		}

		defer rows.Close()

		for rows.Next() {
			var (
				value  float64
				name   string
				labels sampleLabels
				time   int64
			)
			err := rows.Scan(&time, &name, &value, &labels)
			if err != nil {
				return nil, err
			}

			key := labels.key(name)
			ts, ok := labelsToSeries[key]

			if !ok {
				labelPairs := make([]prompb.Label, 0, labels.len()+1)
				labelPairs = append(labelPairs, prompb.Label{
					Name:  model.MetricNameLabel,
					Value: name,
				})

				for _, k := range labels.OrderedKeys {
					labelPairs = append(labelPairs, prompb.Label{
						Name:  k,
						Value: labels.Map[k],
					})
				}

				ts = &prompb.TimeSeries{
					Labels:  labelPairs,
					Samples: make([]prompb.Sample, 0, 100),
				}
				labelsToSeries[key] = ts
			}

			ts.Samples = append(ts.Samples, prompb.Sample{
				Timestamp: time,
				Value:     value,
			})
		}

		err = rows.Err()

		if err != nil {
			return nil, err
		}
	}

	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: make([]*prompb.TimeSeries, 0, len(labelsToSeries)),
			},
		},
	}
	for _, ts := range labelsToSeries {
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
		if c.cfg.s2PrometheusLogSamples {
			log.Debug("timeseries", ts.String())
		}
	}

	log.Debug("msg", "Returned response", "#timeseries", len(labelsToSeries))

	return &resp, nil
}

// HealthCheck implements the healtcheck interface
func (c *Client) HealthCheck() error {
	rows, err := c.DB.Query("SELECT 1")
	if err != nil {
		log.Debug("msg", "Health check error", "err", err)
		return err
	}

	rows.Close()
	return nil
}

func (c *Client) buildQuery(q *prompb.Query) (string, error) {
	matchers := make([]string, 0, len(q.Matchers))
	labelEqualPredicates := make(map[string]string)
	// fmt.Printf("[buildQuery] matchers: %v\n", q.Matchers)

	for _, m := range q.Matchers {
		escapedName := escapeValue(m.Name)
		escapedValue := escapeValue(m.Value)

		if m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				if len(escapedValue) == 0 {
					matchers = append(matchers, fmt.Sprintf("(name IS NULL OR name = '')"))
				} else {
					matchers = append(matchers, fmt.Sprintf("name = '%s'", escapedValue))
				}
			case prompb.LabelMatcher_NEQ:
				matchers = append(matchers, fmt.Sprintf("name != '%s'", escapedValue))
			case prompb.LabelMatcher_RE:
				matchers = append(matchers, fmt.Sprintf("name RLIKE '%s'", anchorValue(escapedValue)))
			case prompb.LabelMatcher_NRE:
				matchers = append(matchers, fmt.Sprintf("name NOT RLIKE '%s'", anchorValue(escapedValue)))
			default:
				return "", fmt.Errorf("unknown metric name match type %v", m.Type)
			}
		} else {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				if len(escapedValue) == 0 {
					// From the PromQL docs: "Label matchers that match
					// empty label values also select all time series that
					// do not have the specific label set at all."
					matchers = append(matchers, fmt.Sprintf("((labels::%s IS NULL) OR (labels::$%s = ''))",
						escapedName, escapedName))
				} else {
					labelEqualPredicates[escapedName] = escapedValue
				}
			case prompb.LabelMatcher_NEQ:
				matchers = append(matchers, fmt.Sprintf("labels::$%s != '%s'", escapedName, escapedValue))
			case prompb.LabelMatcher_RE:
				matchers = append(matchers, fmt.Sprintf("labels::$%s RLIKE '%s'", escapedName, anchorValue(escapedValue)))
			case prompb.LabelMatcher_NRE:
				matchers = append(matchers, fmt.Sprintf("labels::$%s NOT RLIKE '%s'", escapedName, anchorValue(escapedValue)))
			default:
				return "", fmt.Errorf("unknown match type %v", m.Type)
			}
		}
	}
	equalsPredicate := ""

	for key, val := range labelEqualPredicates {
		// fmt.Printf("[labelEqualPredicates] key: %s, value: %s\n", key, val)
		equalsPredicate += fmt.Sprintf(" AND labels::$`%s` = '%s'", key, val)
	}

	matchers = append(matchers, fmt.Sprintf("time >= %v", q.StartTimestampMs))
	matchers = append(matchers, fmt.Sprintf("time <= %v", q.EndTimestampMs))

	return fmt.Sprintf("SELECT time, name, value, labels FROM %s WHERE %s %s ORDER BY time",
		c.cfg.table, strings.Join(matchers, " AND "), equalsPredicate), nil
}

func (c *Client) buildCommand(q *prompb.Query) (string, error) {
	return c.buildQuery(q)
}

func escapeValue(str string) string {
	return strings.Replace(str, `'`, `''`, -1)
}

// anchorValue adds anchors to values in regexps since PromQL docs
// states that "Regex-matches are fully anchored."
func anchorValue(str string) string {
	l := len(str)

	if l == 0 || (str[0] == '^' && str[l-1] == '$') {
		return str
	}

	if str[0] == '^' {
		return fmt.Sprintf("%s$", str)
	}

	if str[l-1] == '$' {
		return fmt.Sprintf("^%s", str)
	}

	return fmt.Sprintf("^%s$", str)
}

// Name identifies the client as a SingleStore client.
func (c Client) Name() string {
	return "SingleStore"
}

// Describe implements prometheus.Collector.
func (c *Client) Describe(ch chan<- *prometheus.Desc) {
}

// Collect implements prometheus.Collector.
func (c *Client) Collect(ch chan<- prometheus.Metric) {
	// ch <- c.ignoredSamples
}
