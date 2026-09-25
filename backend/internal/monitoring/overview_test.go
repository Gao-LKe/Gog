package monitoring

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOverviewUsesOldestKafkaMessageForThreeHourAlert(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if req.URL.Path == "/api/v1/query_range" {
			_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
			return
		}
		query := req.URL.Query().Get("query")
		value := "0"
		if strings.HasPrefix(query, `up{`) || query == "goblog_order_consumer_alive" {
			value = "1"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"result": []any{map[string]any{"metric": map[string]string{}, "value": []any{float64(1), value}}}}})
	}))
	defer server.Close()
	metrics := NewMetrics()
	metrics.SetKafkaSample(KafkaLagSnapshot{ObservedAt: time.Now(), State: "ok", Lag: 8, Overdue: -1, OldestAge: 3*time.Hour + time.Second, Partitions: []KafkaPartitionLag{{Partition: 0, Lag: 8, OldestAge: 3*time.Hour + time.Second}}})
	metrics.SetDBProbe(true)
	// A live SQL pool sample is independent of the Prometheus test server.
	metrics.mu.Lock()
	metrics.hasDB = true
	metrics.dbSampledAt = time.Now()
	metrics.mu.Unlock()
	view, err := NewReader(server.URL, metrics).Overview(context.Background(), "5m")
	if err != nil {
		t.Fatal(err)
	}
	if view.State != "critical" || view.Order.Queue.Lag == nil || *view.Order.Queue.Lag != 8 {
		t.Fatalf("unexpected queue state: %+v", view.Order.Queue)
	}
	if view.Order.Queue.Overdue != nil {
		t.Fatal("distinct overdue order requests must remain unknown")
	}
}

func TestOverviewRejectsUnavailablePrometheus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
	}))
	defer server.Close()
	_, err := NewReader(server.URL, NewMetrics()).Overview(context.Background(), "5m")
	if err == nil {
		t.Fatal("missing scrape target must not be presented as zero traffic")
	}
}

func TestOverviewRejectsFirstScrapeWithoutCounterHistory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		query := req.URL.Query().Get("query")
		if strings.HasPrefix(query, `up{`) {
			_, _ = w.Write([]byte(`{"status":"success","data":{"result":[{"metric":{},"value":[1,"1"]}]}}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
	}))
	defer server.Close()
	_, err := NewReader(server.URL, NewMetrics()).Overview(context.Background(), "1m")
	if err == nil {
		t.Fatal("a single scrape has no reliable one-minute request count")
	}
}
