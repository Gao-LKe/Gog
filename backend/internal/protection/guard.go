package protection

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrBusy        = errors.New("concurrency limit reached")
	ErrCircuitOpen = errors.New("circuit is open")
)

// Bulkhead limits concurrent work without knowing any business details.
type Bulkhead struct{ slots chan struct{} }

func NewBulkhead(limit int) *Bulkhead {
	if limit < 1 {
		limit = 1
	}
	return &Bulkhead{slots: make(chan struct{}, limit)}
}

func (b *Bulkhead) Do(ctx context.Context, fn func(context.Context) error) error {
	select {
	case b.slots <- struct{}{}:
		defer func() { <-b.slots }()
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrBusy
	}
	return fn(ctx)
}

// CircuitBreaker is a compact boundary for protecting external dependencies.
type CircuitBreaker struct {
	mu        sync.Mutex
	failures  int
	threshold int
	openUntil time.Time
	openFor   time.Duration
}

func NewCircuitBreaker(threshold int, openFor time.Duration) *CircuitBreaker {
	if threshold < 1 {
		threshold = 1
	}
	if openFor <= 0 {
		openFor = time.Second
	}
	return &CircuitBreaker{threshold: threshold, openFor: openFor}
}

func (b *CircuitBreaker) Do(ctx context.Context, fn func(context.Context) error) error {
	now := time.Now()
	b.mu.Lock()
	if now.Before(b.openUntil) {
		b.mu.Unlock()
		return ErrCircuitOpen
	}
	b.mu.Unlock()

	err := fn(ctx)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		b.failures = 0
		b.openUntil = time.Time{}
		return nil
	}
	b.failures++
	if b.failures >= b.threshold {
		b.openUntil = now.Add(b.openFor)
		b.failures = 0
	}
	return err
}
