package monitoring

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestPartitionLag(t *testing.T) {
	tests := []struct {
		name                   string
		committed, first, last int64
		want                   int64
		wantError              bool
	}{
		{"empty", 10, 10, 10, 0, false},
		{"backlog", 12, 10, 25, 13, false},
		{"uncommitted", -1, 0, 10, 0, true},
		{"retention gap", 2, 4, 10, 0, true},
		{"future commit", 12, 0, 10, 0, true},
		{"bad log range", 0, -1, 10, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := partitionLag(tc.committed, tc.first, tc.last)
			if (err != nil) != tc.wantError || got != tc.want {
				t.Fatalf("partitionLag(%d, %d, %d) = %d, %v; want %d, error=%v", tc.committed, tc.first, tc.last, got, err, tc.want, tc.wantError)
			}
		})
	}
}

func TestFirstRecordAgeSkipsEarlierBatchRecords(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	records := kafka.NewRecordReader(
		kafka.Record{Offset: 8, Time: now.Add(-5 * time.Hour)},
		kafka.Record{Offset: 9, Time: now.Add(-4 * time.Hour)},
		kafka.Record{Offset: 10, Time: now.Add(-2 * time.Hour)},
	)
	age, err := firstRecordAge(records, 10, now)
	if err != nil || age != 2*time.Hour {
		t.Fatalf("age = %s, %v; want 2h", age, err)
	}
}

func TestFirstRecordAgeRejectsMissingRecordAndTimestamp(t *testing.T) {
	now := time.Now()
	for _, records := range []kafka.RecordReader{
		kafka.NewRecordReader(),
		kafka.NewRecordReader(kafka.Record{Offset: 10}),
	} {
		if _, err := firstRecordAge(records, 10, now); err == nil {
			t.Fatal("expected missing record or timestamp error")
		}
	}
}

func TestSampleKafkaLagMissingConfiguration(t *testing.T) {
	snapshot, err := SampleKafkaLag(context.Background(), nil, "", "", 3*time.Hour)
	if err != nil || snapshot.State != "not_integrated" || snapshot.Overdue != -1 {
		t.Fatalf("missing topic/group = %+v, %v", snapshot, err)
	}
	snapshot, err = SampleKafkaLag(context.Background(), nil, "orders", "group", 3*time.Hour)
	if err == nil || !strings.Contains(err.Error(), "no brokers") || snapshot.State != "unavailable" || snapshot.Overdue != -1 {
		t.Fatalf("missing brokers = %+v, %v", snapshot, err)
	}
}
