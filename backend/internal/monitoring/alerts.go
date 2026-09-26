package monitoring

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

const runtimeSampleMaxAge = 90 * time.Second

// AlertNotifier is deliberately small so monitoring can reuse any delivery
// mechanism without taking a dependency on authentication or SMTP packages.
type AlertNotifier interface {
	SendAlert(context.Context, string, string, string) error
}

type AlertConfig struct {
	Recipient          string
	Cooldown           time.Duration
	ConsecutiveSamples int
	HTTPMinRequests    int64
	HTTPErrorRate      float64
	HTTPP95            time.Duration
}

type HTTPHealth struct {
	State        string
	Completed    int64
	ServerErrors int64
	P95          *time.Duration
}

type RuntimeSnapshot struct {
	ObservedAt      time.Time
	Kafka           KafkaLagSnapshot
	HasKafka        bool
	DBSampledAt     time.Time
	HasDB           bool
	DBHealthy       bool
	ActiveConsumers int
}

type AlertSignals struct {
	Runtime RuntimeSnapshot
	HTTP    HTTPHealth
}

type alertCondition struct {
	Key      string
	Severity string
	Title    string
	Body     string
}

type alertState struct {
	Consecutive int
	Active      bool
	LastSent    time.Time
	Title       string
}

// AlertManager sends the first qualifying alert, then suppresses reminders
// until Cooldown elapses. A single recovery email is sent after a rule clears.
type AlertManager struct {
	cfg      AlertConfig
	notifier AlertNotifier
	now      func() time.Time
	mu       sync.Mutex
	states   map[string]alertState
}

func NewAlertManager(cfg AlertConfig, notifier AlertNotifier) *AlertManager {
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 30 * time.Minute
	}
	if cfg.ConsecutiveSamples <= 0 {
		cfg.ConsecutiveSamples = 2
	}
	if cfg.HTTPMinRequests <= 0 {
		cfg.HTTPMinRequests = 20
	}
	if cfg.HTTPErrorRate <= 0 || cfg.HTTPErrorRate > 1 {
		cfg.HTTPErrorRate = .05
	}
	if cfg.HTTPP95 <= 0 {
		cfg.HTTPP95 = 2 * time.Second
	}
	return &AlertManager{cfg: cfg, notifier: notifier, now: time.Now, states: make(map[string]alertState)}
}

func (m *AlertManager) Evaluate(ctx context.Context, signals AlertSignals) []error {
	if m == nil || m.notifier == nil || strings.TrimSpace(m.cfg.Recipient) == "" {
		return nil
	}
	conditions := m.conditions(signals)
	active := make(map[string]alertCondition, len(conditions))
	for _, condition := range conditions {
		active[condition.Key] = condition
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now().UTC()
	errs := make([]error, 0)
	for key, condition := range active {
		state := m.states[key]
		state.Consecutive++
		if !state.Active && state.Consecutive < m.cfg.ConsecutiveSamples {
			m.states[key] = state
			continue
		}
		if state.Active && !state.LastSent.IsZero() && now.Sub(state.LastSent) < m.cfg.Cooldown {
			m.states[key] = state
			continue
		}
		subject, body := alertMail(condition, false, now)
		if err := m.send(ctx, subject, body); err != nil {
			errs = append(errs, fmt.Errorf("send %s alert: %w", key, err))
			m.states[key] = state
			continue
		}
		state.Active = true
		state.LastSent = now
		state.Title = condition.Title
		m.states[key] = state
	}

	for key, state := range m.states {
		if _, stillActive := active[key]; stillActive || !state.Active {
			continue
		}
		subject, body := alertMail(alertCondition{Key: key, Title: state.Title}, true, now)
		if err := m.send(ctx, subject, body); err != nil {
			errs = append(errs, fmt.Errorf("send %s recovery: %w", key, err))
			continue
		}
		delete(m.states, key)
	}
	return errs
}

func (m *AlertManager) send(parent context.Context, subject, body string) error {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	return m.notifier.SendAlert(ctx, m.cfg.Recipient, subject, body)
}

func (m *AlertManager) conditions(signals AlertSignals) []alertCondition {
	conditions := make([]alertCondition, 0, 6)
	runtime := signals.Runtime
	if runtime.HasKafka {
		switch runtime.Kafka.State {
		case "unavailable":
			conditions = append(conditions, alertCondition{Key: "kafka_sample_unavailable", Severity: "警告", Title: "Kafka 监测采样失败", Body: "无法读取订单消费组积压。请检查 Kafka Broker、Topic 和网络连通性。"})
		case "ok":
			if runtime.Kafka.Lag > 0 && runtime.Kafka.OldestAge >= 3*time.Hour {
				conditions = append(conditions, alertCondition{Key: "order_queue_deadline", Severity: "严重", Title: "订单队列等待超过 3 小时", Body: fmt.Sprintf("当前积压 %d 条，最老未消费消息已等待 %s。", runtime.Kafka.Lag, runtime.Kafka.OldestAge.Round(time.Second))})
			}
			if runtime.Kafka.Lag > 0 && runtime.ActiveConsumers == 0 {
				conditions = append(conditions, alertCondition{Key: "order_consumer_stopped", Severity: "严重", Title: "订单消费者已停止", Body: fmt.Sprintf("当前订单积压 %d 条，但没有运行中的消费者。", runtime.Kafka.Lag)})
			}
		}
	}
	if !runtime.HasDB || !runtime.DBHealthy || time.Since(runtime.DBSampledAt) > runtimeSampleMaxAge {
		conditions = append(conditions, alertCondition{Key: "mysql_unavailable", Severity: "严重", Title: "MySQL 健康检查失败", Body: "数据库连接池探测失败或采样已过期。"})
	}
	if signals.HTTP.State == "unavailable" {
		conditions = append(conditions, alertCondition{Key: "http_monitoring_unavailable", Severity: "警告", Title: "HTTP 监测数据不可用", Body: "Prometheus 无法提供后端接口健康数据。"})
	}
	if signals.HTTP.State == "ok" && signals.HTTP.Completed >= m.cfg.HTTPMinRequests {
		errorRate := float64(signals.HTTP.ServerErrors) / float64(signals.HTTP.Completed)
		if errorRate >= m.cfg.HTTPErrorRate {
			conditions = append(conditions, alertCondition{Key: "http_server_errors", Severity: "警告", Title: "HTTP 5xx 比例过高", Body: fmt.Sprintf("近 5 分钟完成 %d 个请求，其中 %d 个为 5xx，错误率 %.2f%%。", signals.HTTP.Completed, signals.HTTP.ServerErrors, errorRate*100)})
		}
		if signals.HTTP.P95 != nil && *signals.HTTP.P95 >= m.cfg.HTTPP95 {
			conditions = append(conditions, alertCondition{Key: "http_latency_p95", Severity: "警告", Title: "HTTP P95 响应时间过高", Body: fmt.Sprintf("近 5 分钟 P95 响应时间为 %s，阈值为 %s。", signals.HTTP.P95.Round(time.Millisecond), m.cfg.HTTPP95)})
		}
	}
	return conditions
}

func alertMail(condition alertCondition, recovered bool, now time.Time) (string, string) {
	if recovered {
		return "[GoBlog][恢复] " + condition.Title, fmt.Sprintf("告警规则：%s\n状态：已恢复\n恢复时间：%s\n\n系统已连续满足恢复条件。", condition.Key, now.Format(time.RFC3339))
	}
	return "[GoBlog][" + condition.Severity + "] " + condition.Title, fmt.Sprintf("告警规则：%s\n状态：%s\n触发时间：%s\n\n%s", condition.Key, condition.Severity, now.Format(time.RFC3339), condition.Body)
}
