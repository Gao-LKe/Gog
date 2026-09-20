package protection

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFixedWindowLimiter(t *testing.T) {
	l := NewFixedWindowLimiter(1, time.Minute)
	if !l.Allow("client") || l.Allow("client") {
		t.Fatal("expected one request per window")
	}
}

func TestCircuitBreakerOpensAfterThreshold(t *testing.T) {
	b := NewCircuitBreaker(1, time.Minute)
	want := errors.New("dependency failed")
	if err := b.Do(context.Background(), func(context.Context) error { return want }); !errors.Is(err, want) {
		t.Fatalf("unexpected dependency error: %v", err)
	}
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected open circuit, got %v", err)
	}
}
