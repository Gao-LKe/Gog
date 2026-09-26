package main

import (
	"strings"
	"testing"
	"time"
)

func TestPercentileMillisUsesNearestRank(t *testing.T) {
	values := []time.Duration{30 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	if got := percentileMillis(values, .95); got == nil || *got != 40 {
		t.Fatalf("p95 = %v, want 40", got)
	}
	if got := percentileMillis(values, .5); got == nil || *got != 20 {
		t.Fatalf("p50 = %v, want 20", got)
	}
}

func TestRequestedCount(t *testing.T) {
	if got := requestedCount(125, 90*time.Second); got != 11_250 {
		t.Fatalf("count = %d, want 11250", got)
	}
	if got := requestedCount(0, time.Second); got != 0 {
		t.Fatalf("count = %d, want 0", got)
	}
}

func TestNormalizeBaseURL(t *testing.T) {
	if got, err := normalizeBaseURL("http://localhost:8081/api/"); err != nil || got != "http://localhost:8081/api" {
		t.Fatalf("url = %q, error = %v", got, err)
	}
	if _, err := normalizeBaseURL("localhost:8081"); err == nil {
		t.Fatal("relative URL was accepted")
	}
}

func TestParseHistogramP99FiltersOperation(t *testing.T) {
	metrics := `# TYPE goblog_db_transaction_duration_seconds histogram
goblog_db_transaction_duration_seconds_bucket{operation="order_create",result="success",le="0.005"} 90
goblog_db_transaction_duration_seconds_bucket{operation="order_create",result="success",le="0.01"} 99
goblog_db_transaction_duration_seconds_bucket{operation="order_create",result="success",le="0.025"} 100
goblog_db_transaction_duration_seconds_bucket{operation="order_create",result="success",le="+Inf"} 100
goblog_db_transaction_duration_seconds_bucket{operation="payment_create",result="success",le="0.005"} 100
`
	value := parseHistogramP99(strings.NewReader(metrics), "goblog_db_transaction_duration_seconds", "order_create")
	if value == nil || *value != 10 {
		t.Fatalf("p99 = %v, want 10 ms", value)
	}
}

func TestListingIDForRequestSpreadsHotTraffic(t *testing.T) {
	loaded := profile{
		ListingIDs:        []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		HotListingCount:   5,
		HotRequestPercent: 30,
	}
	hot, ordinary := 0, 0
	for index := 0; index < 100; index++ {
		listingID := loaded.listingIDForRequest(index)
		if listingID <= 5 {
			hot++
		} else {
			ordinary++
		}
	}
	if hot != 30 || ordinary != 70 {
		t.Fatalf("hot/ordinary = %d/%d, want 30/70", hot, ordinary)
	}
	for start := 0; start < 100; start += 10 {
		count := 0
		for index := start; index < start+10; index++ {
			if loaded.listingIDForRequest(index) <= 5 {
				count++
			}
		}
		if count != 3 {
			t.Fatalf("requests %d-%d contain %d hot requests, want 3", start, start+9, count)
		}
	}
}

func TestPhaseReportKeepsActualInputDuration(t *testing.T) {
	accumulator := newPhaseAccumulator()
	accumulator.markStarted(time.Unix(100, 0))
	accumulator.markStarted(time.Unix(101, 0))
	accumulator.record("transport_error", time.Millisecond, false)
	result := accumulator.report(100, 10*time.Second)
	if result.InputDuration <= 0 {
		t.Fatal("input duration was not recorded")
	}
}
