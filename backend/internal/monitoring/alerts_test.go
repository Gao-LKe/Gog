package monitoring

import (
	"context"
	"sync"
	"testing"
	"time"
)

type recordedAlert struct {
	recipient string
	subject   string
	body      string
}

type memoryAlertNotifier struct {
	mu     sync.Mutex
	alerts []recordedAlert
}

func (n *memoryAlertNotifier) SendAlert(_ context.Context, recipient, subject, body string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.alerts = append(n.alerts, recordedAlert{recipient: recipient, subject: subject, body: body})
	return nil
}

func (n *memoryAlertNotifier) all() []recordedAlert {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]recordedAlert(nil), n.alerts...)
}

func TestAlertManagerSendsOnceThenRecoveryAfterConsecutiveSamples(t *testing.T) {
	notifier := &memoryAlertNotifier{}
	manager := NewAlertManager(AlertConfig{Recipient: "admin@example.test", ConsecutiveSamples: 2, Cooldown: time.Hour}, notifier)
	signals := AlertSignals{Runtime: RuntimeSnapshot{HasKafka: true, Kafka: KafkaLagSnapshot{State: "ok", Lag: 4, OldestAge: 3*time.Hour + time.Second}, HasDB: true, DBHealthy: true, DBSampledAt: time.Now(), ActiveConsumers: 1}, HTTP: HTTPHealth{State: "ok"}}
	if errs := manager.Evaluate(context.Background(), signals); len(errs) != 0 {
		t.Fatal(errs)
	}
	if got := len(notifier.all()); got != 0 {
		t.Fatalf("first sample sent %d alerts", got)
	}
	manager.Evaluate(context.Background(), signals)
	manager.Evaluate(context.Background(), signals)
	alerts := notifier.all()
	if len(alerts) != 1 || alerts[0].recipient != "admin@example.test" || alerts[0].subject != "[GoBlog][严重] 订单队列等待超过 3 小时" {
		t.Fatalf("unexpected active alerts: %+v", alerts)
	}
	signals.Runtime.Kafka.OldestAge = time.Hour
	manager.Evaluate(context.Background(), signals)
	alerts = notifier.all()
	if len(alerts) != 2 || alerts[1].subject != "[GoBlog][恢复] 订单队列等待超过 3 小时" {
		t.Fatalf("unexpected recovery alerts: %+v", alerts)
	}
}

func TestAlertManagerWaitsForEnoughHTTPRequests(t *testing.T) {
	notifier := &memoryAlertNotifier{}
	manager := NewAlertManager(AlertConfig{Recipient: "admin@example.test", ConsecutiveSamples: 1, HTTPMinRequests: 20, HTTPErrorRate: .05}, notifier)
	signals := AlertSignals{Runtime: RuntimeSnapshot{HasDB: true, DBHealthy: true, DBSampledAt: time.Now()}, HTTP: HTTPHealth{State: "ok", Completed: 19, ServerErrors: 19}}
	manager.Evaluate(context.Background(), signals)
	if got := len(notifier.all()); got != 0 {
		t.Fatalf("small HTTP sample sent %d alerts", got)
	}
	signals.HTTP.Completed = 20
	signals.HTTP.ServerErrors = 1
	manager.Evaluate(context.Background(), signals)
	alerts := notifier.all()
	if len(alerts) != 1 || alerts[0].subject != "[GoBlog][警告] HTTP 5xx 比例过高" {
		t.Fatalf("unexpected HTTP alert: %+v", alerts)
	}
}
