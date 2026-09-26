package monitoring

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//===========================================
//      系统运行指标
//===========================================

type Metrics struct {
	registry        *prometheus.Registry
	httpReceived    prometheus.Counter
	httpCompleted   *prometheus.CounterVec
	httpDuration    *prometheus.HistogramVec
	httpInflight    prometheus.Gauge
	orderSubmit     *prometheus.CounterVec
	submitDuration  *prometheus.HistogramVec
	orderProcess    *prometheus.CounterVec
	processTime     *prometheus.HistogramVec
	consumer        *prometheus.CounterVec
	consumerAlive   prometheus.Gauge
	consumerWorkers prometheus.Gauge
	dbTransaction   *prometheus.HistogramVec
	kafkaLag        *prometheus.GaugeVec
	kafkaOldest     *prometheus.GaugeVec
	kafkaProbe      prometheus.Gauge
	kafkaSampleAt   prometheus.Gauge
	dbOpen          prometheus.Gauge
	dbInUse         prometheus.Gauge
	dbWaitCount     prometheus.Gauge
	dbWaitSeconds   prometheus.Gauge
	dbSampleAt      prometheus.Gauge
	dbProbe         prometheus.Gauge
	mu              sync.RWMutex
	latestKafka     KafkaLagSnapshot
	hasKafka        bool
	latestDB        sql.DBStats
	dbSampledAt     time.Time
	hasDB           bool
	dbHealthy       bool
	activeConsumers int
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry: registry,
		httpReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "goblog", Subsystem: "http", Name: "received_total", Help: "HTTP requests received by the application.",
		}),
		httpCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goblog", Subsystem: "http", Name: "completed_total", Help: "Completed HTTP requests by route and status class.",
		}, []string{"method", "route", "status_class"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "goblog", Subsystem: "http", Name: "request_duration_seconds", Help: "HTTP handler duration in seconds.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"method", "route"}),
		httpInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "http", Name: "inflight", Help: "HTTP requests currently being handled.",
		}),
		orderSubmit: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goblog", Subsystem: "order", Name: "submit_total", Help: "Order submission attempts by result.",
		}, []string{"result"}),
		submitDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "goblog", Subsystem: "order", Name: "publish_duration_seconds", Help: "Order command publication duration.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"result"}),
		orderProcess: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goblog", Subsystem: "order", Name: "process_total", Help: "Order message processing attempts by outcome; duplicates are counted separately.",
		}, []string{"result"}),
		processTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "goblog", Subsystem: "order", Name: "process_duration_seconds", Help: "Order message processing duration.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"result"}),
		consumer: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goblog", Subsystem: "order", Name: "consumer_total", Help: "Order consumer events by result.",
		}, []string{"result"}),
		consumerAlive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "order", Name: "consumer_alive", Help: "Whether the order consumer loop is running.",
		}),
		consumerWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "order", Name: "consumer_workers", Help: "Order consumer worker loops currently running.",
		}),
		dbTransaction: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "goblog", Name: "db_transaction_duration_seconds", Help: "Database transaction duration by business operation.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"operation", "result"}),
		kafkaLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "order", Name: "kafka_lag", Help: "Uncommitted order commands by partition.",
		}, []string{"partition"}),
		kafkaOldest: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "order", Name: "kafka_oldest_seconds", Help: "Age of oldest uncommitted order command by partition.",
		}, []string{"partition"}),
		kafkaProbe: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "order", Name: "kafka_probe_ok", Help: "Whether the latest Kafka lag sample succeeded.",
		}),
		kafkaSampleAt: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "order", Name: "kafka_sample_timestamp_seconds", Help: "Unix time of latest successful Kafka lag sample.",
		}),
		dbOpen: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "mysql", Name: "pool_open", Help: "Open SQL connections in the application pool.",
		}),
		dbInUse: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "mysql", Name: "pool_in_use", Help: "SQL connections currently in use.",
		}),
		dbWaitCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "mysql", Name: "pool_wait_count", Help: "Cumulative number of waits for a SQL connection since process start.",
		}),
		dbWaitSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "mysql", Name: "pool_wait_seconds", Help: "Cumulative SQL connection wait time since process start.",
		}),
		dbSampleAt: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "mysql", Name: "pool_sample_timestamp_seconds", Help: "Unix time of latest SQL pool sample.",
		}),
		dbProbe: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goblog", Subsystem: "mysql", Name: "probe_ok", Help: "Whether the latest MySQL ping succeeded.",
		}),
	}
	registry.MustRegister(m.httpReceived, m.httpCompleted, m.httpDuration, m.httpInflight,
		m.orderSubmit, m.submitDuration, m.orderProcess, m.processTime, m.consumer, m.consumerAlive, m.consumerWorkers, m.dbTransaction,
		m.kafkaLag, m.kafkaOldest, m.kafkaProbe, m.kafkaSampleAt,
		m.dbOpen, m.dbInUse, m.dbWaitCount, m.dbWaitSeconds, m.dbSampleAt, m.dbProbe)
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	for _, result := range []string{"accepted", "invalid", "failed"} {
		m.orderSubmit.WithLabelValues(result)
	}
	for _, result := range []string{"created", "rejected", "duplicate", "failed"} {
		m.orderProcess.WithLabelValues(result)
	}
	for _, result := range []string{"fetched", "decode_failed", "process_failed", "committed", "commit_failed"} {
		m.consumer.WithLabelValues(result)
	}
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) HTTPMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/healthz" || c.Request.URL.Path == "/readyz" || c.Request.URL.Path == "/api/v1/monitoring/overview" || strings.EqualFold(c.GetHeader("Upgrade"), "websocket") {
			c.Next()
			return
		}
		start := time.Now()
		m.httpReceived.Inc()
		m.httpInflight.Inc()
		defer m.httpInflight.Dec()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		status := c.Writer.Status()
		class := statusClass(status)
		m.httpCompleted.WithLabelValues(c.Request.Method, route, class).Inc()
		m.httpDuration.WithLabelValues(c.Request.Method, route).Observe(time.Since(start).Seconds())
	}
}

func statusClass(status int) string {
	switch {
	case status == http.StatusTooManyRequests:
		return "429"
	case status >= 200 && status < 300:
		return "2xx"
	case status >= 300 && status < 400:
		return "3xx"
	case status >= 400 && status < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

func (m *Metrics) OrderSubmitted(result string, duration time.Duration) {
	m.orderSubmit.WithLabelValues(result).Inc()
	if result != "invalid" {
		m.submitDuration.WithLabelValues(result).Observe(duration.Seconds())
	}
}

func (m *Metrics) OrderProcessed(result string, duration time.Duration) {
	m.orderProcess.WithLabelValues(result).Inc()
	m.processTime.WithLabelValues(result).Observe(duration.Seconds())
}

func (m *Metrics) DBTransaction(operation, result string, duration time.Duration) {
	m.dbTransaction.WithLabelValues(operation, result).Observe(duration.Seconds())
}

func (m *Metrics) OrderConsumer(result string) { m.consumer.WithLabelValues(result).Inc() }
func (m *Metrics) ConsumerAlive(alive bool) {
	m.mu.Lock()
	if alive {
		m.activeConsumers++
	} else if m.activeConsumers > 0 {
		m.activeConsumers--
	}
	active := m.activeConsumers
	m.mu.Unlock()
	m.consumerWorkers.Set(float64(active))
	if active > 0 {
		m.consumerAlive.Set(1)
	} else {
		m.consumerAlive.Set(0)
	}
}

func (m *Metrics) SampleDB(db *sql.DB) {
	if db == nil {
		return
	}
	stats := db.Stats()
	m.dbOpen.Set(float64(stats.OpenConnections))
	m.dbInUse.Set(float64(stats.InUse))
	m.dbWaitCount.Set(float64(stats.WaitCount))
	m.dbWaitSeconds.Set(stats.WaitDuration.Seconds())
	m.dbSampleAt.SetToCurrentTime()
	m.mu.Lock()
	m.latestDB = stats
	m.dbSampledAt = time.Now()
	m.hasDB = true
	m.mu.Unlock()
}

func (m *Metrics) LatestDB() (sql.DBStats, time.Time, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestDB, m.dbSampledAt, m.hasDB
}

func (m *Metrics) SetDBProbe(ok bool) {
	m.mu.Lock()
	m.dbHealthy = ok
	m.mu.Unlock()
	if ok {
		m.dbProbe.Set(1)
	} else {
		m.dbProbe.Set(0)
	}
}

func (m *Metrics) DBHealthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dbHealthy
}

func (m *Metrics) SetKafkaSample(sample KafkaLagSnapshot) {
	m.mu.Lock()
	m.latestKafka = sample
	m.hasKafka = true
	m.mu.Unlock()
	if sample.State != "ok" {
		m.kafkaProbe.Set(0)
		m.kafkaLag.Reset()
		m.kafkaOldest.Reset()
		return
	}
	m.kafkaProbe.Set(1)
	m.kafkaSampleAt.Set(float64(sample.ObservedAt.Unix()))
	m.kafkaLag.Reset()
	m.kafkaOldest.Reset()
	for _, partition := range sample.Partitions {
		label := strconv.Itoa(partition.Partition)
		m.kafkaLag.WithLabelValues(label).Set(float64(partition.Lag))
		m.kafkaOldest.WithLabelValues(label).Set(partition.OldestAge.Seconds())
	}
}

func (m *Metrics) LatestKafka() (KafkaLagSnapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestKafka, m.hasKafka
}

func (m *Metrics) RuntimeSnapshot() RuntimeSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return RuntimeSnapshot{
		ObservedAt:      time.Now().UTC(),
		Kafka:           m.latestKafka,
		HasKafka:        m.hasKafka,
		DBSampledAt:     m.dbSampledAt,
		HasDB:           m.hasDB,
		DBHealthy:       m.dbHealthy,
		ActiveConsumers: m.activeConsumers,
	}
}
