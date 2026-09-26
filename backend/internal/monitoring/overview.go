package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

//===========================================
//      管理员监测视图
//===========================================

type Overview struct {
	ObservedAt time.Time     `json:"observed_at"`
	Window     string        `json:"window"`
	State      string        `json:"state"`
	HTTP       HTTPOverview  `json:"http"`
	Order      OrderOverview `json:"order"`
	MySQL      MySQLOverview `json:"mysql"`
}

type HTTPOverview struct {
	Received     int64          `json:"received"`
	Completed    int64          `json:"completed"`
	Success      int64          `json:"success"`
	Redirects    int64          `json:"redirects"`
	ClientErrors int64          `json:"client_errors"`
	ServerErrors int64          `json:"server_errors"`
	RateLimited  int64          `json:"rate_limited"`
	RPS          float64        `json:"rps"`
	AvgMS        *float64       `json:"avg_ms"`
	P95MS        *float64       `json:"p95_ms"`
	P99MS        *float64       `json:"p99_ms"`
	Routes       []HTTPRoute    `json:"routes"`
	History      []HistoryPoint `json:"history"`
}

type HTTPRoute struct {
	Method       string   `json:"method"`
	Route        string   `json:"route"`
	Completed    int64    `json:"completed"`
	Success      int64    `json:"success"`
	ClientErrors int64    `json:"client_errors"`
	ServerErrors int64    `json:"server_errors"`
	AvgMS        *float64 `json:"avg_ms"`
	P95MS        *float64 `json:"p95_ms"`
}

type HistoryPoint struct {
	At       time.Time `json:"at"`
	RPS      float64   `json:"rps"`
	ErrorRPS float64   `json:"error_rps"`
}

type OrderOverview struct {
	Input  OrderInputOverview  `json:"input"`
	Queue  OrderQueueOverview  `json:"queue"`
	Output OrderOutputOverview `json:"output"`
}

type OrderInputOverview struct {
	State        string   `json:"state"`
	Submitted    int64    `json:"submitted"`
	Accepted     int64    `json:"accepted"`
	Invalid      int64    `json:"invalid"`
	Failed       int64    `json:"failed"`
	AvgPublishMS *float64 `json:"avg_publish_ms"`
}

type OrderQueueOverview struct {
	State         string               `json:"state"`
	Lag           *int64               `json:"lag"`
	OldestSeconds *float64             `json:"oldest_seconds"`
	Overdue       *int64               `json:"overdue"`
	Partitions    []OrderPartitionView `json:"partitions"`
}

type OrderPartitionView struct {
	Partition     int      `json:"partition"`
	Lag           int64    `json:"lag"`
	OldestSeconds *float64 `json:"oldest_seconds"`
}

type OrderOutputOverview struct {
	State     string   `json:"state"`
	Processed int64    `json:"processed"`
	Created   int64    `json:"created"`
	Rejected  int64    `json:"rejected"`
	Failed    int64    `json:"failed"`
	AvgMS     *float64 `json:"avg_ms"`
}

type MySQLOverview struct {
	State           string   `json:"state"`
	OpenConnections *int     `json:"open_connections"`
	InUse           *int     `json:"in_use"`
	WaitCount       *int64   `json:"wait_count"`
	WaitSeconds     *float64 `json:"wait_seconds"`
}

type Reader struct {
	baseURL string
	client  *http.Client
	metrics *Metrics
}

func NewReader(baseURL string, metrics *Metrics) *Reader {
	return &Reader{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 3 * time.Second},
		metrics: metrics,
	}
}

func ValidWindow(window string) (time.Duration, bool) {
	switch window {
	case "1m":
		return time.Minute, true
	case "5m":
		return 5 * time.Minute, true
	case "1h":
		return time.Hour, true
	default:
		return 0, false
	}
}

// HTTPHealth returns the small fixed query set used by the alert evaluator.
// A lack of completed requests is healthy but has insufficient volume for an
// error-rate or latency alert.
func (r *Reader) HTTPHealth(ctx context.Context) (HTTPHealth, error) {
	if r.baseURL == "" {
		return HTTPHealth{State: "unavailable"}, errors.New("monitoring is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	up, err := r.queryVector(ctx, `up{job="backend"}`)
	if err != nil {
		return HTTPHealth{State: "unavailable"}, err
	}
	if len(up) == 0 || up[0].Value == nil || *up[0].Value < 1 {
		return HTTPHealth{State: "unavailable"}, errors.New("monitoring target is unavailable")
	}
	completed, err := r.queryScalar(ctx, `sum(increase(goblog_http_completed_total[5m]))`)
	if err != nil {
		return HTTPHealth{State: "unavailable"}, err
	}
	if completed == nil {
		return HTTPHealth{State: "ok"}, nil
	}
	serverErrors, err := r.queryScalar(ctx, `sum(increase(goblog_http_completed_total{status_class="5xx"}[5m]))`)
	if err != nil {
		return HTTPHealth{State: "unavailable"}, err
	}
	p95MS, err := r.queryScalar(ctx, `1000 * histogram_quantile(0.95,sum by(le)(rate(goblog_http_request_duration_seconds_bucket[5m])))`)
	if err != nil {
		return HTTPHealth{State: "unavailable"}, err
	}
	health := HTTPHealth{State: "ok", Completed: rounded(completed), ServerErrors: rounded(serverErrors)}
	if p95MS != nil {
		value := time.Duration(*p95MS * float64(time.Millisecond))
		health.P95 = &value
	}
	return health, nil
}

func (r *Reader) Overview(ctx context.Context, window string) (Overview, error) {
	duration, ok := ValidWindow(window)
	if !ok {
		return Overview{}, errors.New("invalid monitoring window")
	}
	if r.baseURL == "" || r.metrics == nil {
		return Overview{}, errors.New("monitoring is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	up, err := r.queryVector(ctx, `up{job="backend"}`)
	if err != nil {
		return Overview{}, err
	}
	if len(up) == 0 || up[0].Value == nil || *up[0].Value < 1 {
		return Overview{}, errors.New("monitoring target is unavailable")
	}
	result := Overview{ObservedAt: time.Now().UTC(), Window: window, State: "ok"}
	if err := r.fillHTTP(ctx, window, duration, &result.HTTP); err != nil {
		return Overview{}, err
	}
	if err := r.fillOrder(ctx, window, &result.Order); err != nil {
		return Overview{}, err
	}
	r.fillLocal(&result)
	if result.Order.Queue.State != "ok" || result.Order.Output.State != "ok" || result.MySQL.State != "ok" {
		result.State = "warning"
	}
	if result.Order.Queue.OldestSeconds != nil && *result.Order.Queue.OldestSeconds >= (3*time.Hour).Seconds() {
		result.State = "critical"
	}
	return result, nil
}

func (r *Reader) fillHTTP(ctx context.Context, window string, duration time.Duration, out *HTTPOverview) error {
	out.Routes = make([]HTTPRoute, 0)
	out.History = make([]HistoryPoint, 0)
	received, err := r.queryScalar(ctx, `increase(goblog_http_received_total[`+window+`])`)
	if err != nil {
		return err
	}
	if received == nil {
		return errors.New("monitoring has insufficient HTTP samples")
	}
	out.Received = rounded(received)
	statusRows, err := r.queryVector(ctx, `sum by(method,route,status_class)(increase(goblog_http_completed_total[`+window+`]))`)
	if err != nil {
		return err
	}
	routes := map[string]*HTTPRoute{}
	for _, row := range statusRows {
		n := rounded(row.Value)
		out.Completed += n
		method, route := row.Metric["method"], row.Metric["route"]
		key := method + "\x00" + route
		item := routes[key]
		if item == nil {
			item = &HTTPRoute{Method: method, Route: route}
			routes[key] = item
		}
		item.Completed += n
		switch row.Metric["status_class"] {
		case "2xx":
			out.Success += n
			item.Success += n
		case "3xx":
			out.Redirects += n
		case "4xx":
			out.ClientErrors += n
			item.ClientErrors += n
		case "429":
			out.ClientErrors += n
			out.RateLimited += n
			item.ClientErrors += n
		case "5xx":
			out.ServerErrors += n
			item.ServerErrors += n
		}
	}
	out.RPS = float64(out.Completed) / duration.Seconds()
	avg, err := r.queryScalar(ctx, `1000 * sum(increase(goblog_http_request_duration_seconds_sum[`+window+`])) / sum(increase(goblog_http_request_duration_seconds_count[`+window+`]))`)
	if err != nil {
		return err
	}
	out.AvgMS = avg
	p95, err := r.queryScalar(ctx, `1000 * histogram_quantile(0.95,sum by(le)(rate(goblog_http_request_duration_seconds_bucket[`+window+`])))`)
	if err != nil {
		return err
	}
	out.P95MS = p95
	p99, err := r.queryScalar(ctx, `1000 * histogram_quantile(0.99,sum by(le)(rate(goblog_http_request_duration_seconds_bucket[`+window+`])))`)
	if err != nil {
		return err
	}
	out.P99MS = p99
	if err := r.fillRouteDurations(ctx, window, routes); err != nil {
		return err
	}
	for _, route := range routes {
		out.Routes = append(out.Routes, *route)
	}
	sort.Slice(out.Routes, func(i, j int) bool {
		if out.Routes[i].Completed != out.Routes[j].Completed {
			return out.Routes[i].Completed > out.Routes[j].Completed
		}
		return out.Routes[i].Method+out.Routes[i].Route < out.Routes[j].Method+out.Routes[j].Route
	})
	if len(out.Routes) > 50 {
		out.Routes = out.Routes[:50]
	}
	return r.fillHistory(ctx, duration, out)
}

func (r *Reader) fillRouteDurations(ctx context.Context, window string, routes map[string]*HTTPRoute) error {
	means, err := r.queryVector(ctx, `1000 * sum by(method,route)(increase(goblog_http_request_duration_seconds_sum[`+window+`])) / sum by(method,route)(increase(goblog_http_request_duration_seconds_count[`+window+`]))`)
	if err != nil {
		return err
	}
	for _, row := range means {
		if item := routes[row.Metric["method"]+"\x00"+row.Metric["route"]]; item != nil {
			item.AvgMS = row.Value
		}
	}
	p95, err := r.queryVector(ctx, `1000 * histogram_quantile(0.95,sum by(method,route,le)(rate(goblog_http_request_duration_seconds_bucket[`+window+`])))`)
	if err != nil {
		return err
	}
	for _, row := range p95 {
		if item := routes[row.Metric["method"]+"\x00"+row.Metric["route"]]; item != nil {
			item.P95MS = row.Value
		}
	}
	return nil
}

func (r *Reader) fillHistory(ctx context.Context, duration time.Duration, out *HTTPOverview) error {
	end := time.Now().UTC()
	step := 15 * time.Second
	if duration == time.Hour {
		step = time.Minute
	}
	requests, err := r.queryRange(ctx, `sum(rate(goblog_http_completed_total[1m]))`, end.Add(-duration), end, step)
	if err != nil {
		return err
	}
	failures, err := r.queryRange(ctx, `sum(rate(goblog_http_completed_total{status_class="5xx"}[1m]))`, end.Add(-duration), end, step)
	if err != nil {
		return err
	}
	points := map[int64]*HistoryPoint{}
	for _, p := range requests {
		points[p.At.Unix()] = &HistoryPoint{At: p.At, RPS: p.Value}
	}
	for _, p := range failures {
		if existing := points[p.At.Unix()]; existing != nil {
			existing.ErrorRPS = p.Value
		}
	}
	for _, p := range points {
		out.History = append(out.History, *p)
	}
	sort.Slice(out.History, func(i, j int) bool { return out.History[i].At.Before(out.History[j].At) })
	return nil
}

func (r *Reader) fillOrder(ctx context.Context, window string, out *OrderOverview) error {
	input, err := r.queryVector(ctx, `sum by(result)(increase(goblog_order_submit_total[`+window+`]))`)
	if err != nil {
		return err
	}
	out.Input.State = "ok"
	for _, row := range input {
		n := rounded(row.Value)
		out.Input.Submitted += n
		switch row.Metric["result"] {
		case "accepted":
			out.Input.Accepted += n
		case "invalid":
			out.Input.Invalid += n
		case "failed":
			out.Input.Failed += n
		}
	}
	inputAvg, err := r.queryScalar(ctx, `1000 * sum(increase(goblog_order_publish_duration_seconds_sum[`+window+`])) / sum(increase(goblog_order_publish_duration_seconds_count[`+window+`]))`)
	if err != nil {
		return err
	}
	out.Input.AvgPublishMS = inputAvg
	processed, err := r.queryVector(ctx, `sum by(result)(increase(goblog_order_process_total[`+window+`]))`)
	if err != nil {
		return err
	}
	out.Output.State = "ok"
	for _, row := range processed {
		n := rounded(row.Value)
		out.Output.Processed += n
		switch row.Metric["result"] {
		case "created":
			out.Output.Created += n
		case "rejected":
			out.Output.Rejected += n
		case "failed":
			out.Output.Failed += n
		}
	}
	outputAvg, err := r.queryScalar(ctx, `1000 * sum(increase(goblog_order_process_duration_seconds_sum[`+window+`])) / sum(increase(goblog_order_process_duration_seconds_count[`+window+`]))`)
	if err != nil {
		return err
	}
	out.Output.AvgMS = outputAvg
	alive, err := r.queryScalar(ctx, `goblog_order_consumer_alive`)
	if err != nil {
		return err
	}
	if alive == nil || *alive < 1 {
		out.Output.State = "unavailable"
	}
	return nil
}

func (r *Reader) fillLocal(out *Overview) {
	queue := &out.Order.Queue
	queue.Partitions = make([]OrderPartitionView, 0)
	sample, ok := r.metrics.LatestKafka()
	if !ok || time.Since(sample.ObservedAt) > time.Minute {
		queue.State = "unavailable"
	} else {
		queue.State = sample.State
		if sample.State == "ok" {
			queue.Lag = &sample.Lag
			if sample.Overdue >= 0 {
				queue.Overdue = &sample.Overdue
			}
			oldest := sample.OldestAge.Seconds()
			queue.OldestSeconds = &oldest
			for _, partition := range sample.Partitions {
				age := partition.OldestAge.Seconds()
				queue.Partitions = append(queue.Partitions, OrderPartitionView{Partition: partition.Partition, Lag: partition.Lag, OldestSeconds: &age})
			}
		}
	}
	db := &out.MySQL
	stats, sampledAt, ok := r.metrics.LatestDB()
	if !ok || time.Since(sampledAt) > time.Minute || !r.metrics.DBHealthy() {
		db.State = "unavailable"
		return
	}
	db.State = "ok"
	db.OpenConnections = &stats.OpenConnections
	db.InUse = &stats.InUse
	db.WaitCount = &stats.WaitCount
	waitSeconds := stats.WaitDuration.Seconds()
	db.WaitSeconds = &waitSeconds
}

type promSample struct {
	Metric map[string]string
	Value  *float64
}

type rangePoint struct {
	At    time.Time
	Value float64
}

type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string   `json:"metric"`
			Value  []json.RawMessage   `json:"value"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func (r *Reader) call(ctx context.Context, path string, params url.Values) (promResponse, error) {
	var payload promResponse
	endpoint := r.baseURL + path + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return payload, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return payload, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return payload, fmt.Errorf("prometheus returned %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&payload); err != nil {
		return payload, err
	}
	if payload.Status != "success" {
		return payload, errors.New("prometheus query failed")
	}
	return payload, nil
}

func (r *Reader) queryVector(ctx context.Context, expression string) ([]promSample, error) {
	response, err := r.call(ctx, "/api/v1/query", url.Values{"query": {expression}})
	if err != nil {
		return nil, err
	}
	result := make([]promSample, 0, len(response.Data.Result))
	for _, row := range response.Data.Result {
		result = append(result, promSample{Metric: row.Metric, Value: parsePromValue(row.Value)})
	}
	return result, nil
}

func (r *Reader) queryScalar(ctx context.Context, expression string) (*float64, error) {
	rows, err := r.queryVector(ctx, expression)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0].Value, nil
}

func (r *Reader) queryRange(ctx context.Context, expression string, start, end time.Time, step time.Duration) ([]rangePoint, error) {
	params := url.Values{
		"query": {expression},
		"start": {strconv.FormatFloat(float64(start.Unix()), 'f', -1, 64)},
		"end":   {strconv.FormatFloat(float64(end.Unix()), 'f', -1, 64)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	response, err := r.call(ctx, "/api/v1/query_range", params)
	if err != nil {
		return nil, err
	}
	points := make([]rangePoint, 0)
	for _, row := range response.Data.Result {
		for _, raw := range row.Values {
			if len(raw) != 2 {
				continue
			}
			at, err := strconv.ParseFloat(string(raw[0]), 64)
			if err != nil {
				continue
			}
			value := parsePromValue(raw)
			if value != nil {
				points = append(points, rangePoint{At: time.Unix(int64(at), 0).UTC(), Value: *value})
			}
		}
	}
	return points, nil
}

func parsePromValue(raw []json.RawMessage) *float64 {
	if len(raw) != 2 {
		return nil
	}
	var text string
	if err := json.Unmarshal(raw[1], &text); err != nil {
		return nil
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	return &value
}

func rounded(value *float64) int64 {
	if value == nil || *value < 0 {
		return 0
	}
	return int64(math.Round(*value))
}
