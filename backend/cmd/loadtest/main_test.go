package main

import (
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
