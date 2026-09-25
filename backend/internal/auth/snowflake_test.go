package auth

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewSnowflakeValidatesNode(t *testing.T) {
	for _, node := range []int64{-1, snowflakeMaxNodeID + 1} {
		if _, err := NewSnowflake(node); !errors.Is(err, ErrInvalidSnowflakeNode) {
			t.Fatalf("node %d: expected validation error, got %v", node, err)
		}
	}
	if _, err := NewSnowflake(0); err != nil {
		t.Fatalf("node 0: %v", err)
	}
	if _, err := NewSnowflake(snowflakeMaxNodeID); err != nil {
		t.Fatalf("maximum node: %v", err)
	}
}

func TestSnowflakeConcurrentIDsAreUniqueAndPositive(t *testing.T) {
	var calls atomic.Int64
	clock := func() time.Time {
		// Advance occasionally so the test can exceed one millisecond's sequence.
		return time.UnixMilli(snowflakeEpochUnixMilli + 100 + calls.Add(1)/3000)
	}
	generator, err := NewSnowflakeWithClock(7, clock)
	if err != nil {
		t.Fatal(err)
	}

	const count = 10000
	ids := make(chan uint64, count)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < count/16; j++ {
				id, generateErr := generator.NextID()
				if generateErr != nil {
					t.Errorf("NextID: %v", generateErr)
					return
				}
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[uint64]struct{}, count)
	for id := range ids {
		if id == 0 {
			t.Fatal("generated zero ID")
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate ID %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("got %d IDs, want %d", len(seen), count)
	}
}

func TestSnowflakeEpochProducesPositiveIDs(t *testing.T) {
	clock := func() time.Time { return time.UnixMilli(snowflakeEpochUnixMilli) }
	generator, err := NewSnowflakeWithClock(0, clock)
	if err != nil {
		t.Fatal(err)
	}

	first, err := generator.NextID()
	if err != nil {
		t.Fatal(err)
	}
	if first == 0 {
		t.Fatal("epoch generated zero ID")
	}
	second, err := generator.NextID()
	if err != nil {
		t.Fatal(err)
	}
	if second == 0 || second == first {
		t.Fatalf("epoch IDs must be positive and unique: first=%d second=%d", first, second)
	}
}

func TestSnowflakeSequenceOverflowWaitsForNextMillisecond(t *testing.T) {
	var mu sync.Mutex
	current := snowflakeEpochUnixMilli + 200
	waitCalls := 0
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return time.UnixMilli(current)
	}
	wait := func(time.Duration) {
		mu.Lock()
		current++
		waitCalls++
		mu.Unlock()
	}
	generator, err := NewSnowflakeWithClockAndWaiter(1, clock, wait)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= int(snowflakeMaxSequence); i++ {
		if _, err := generator.NextID(); err != nil {
			t.Fatalf("ID %d: %v", i, err)
		}
	}
	last, err := generator.NextID()
	if err != nil {
		t.Fatal(err)
	}
	if waitCalls != 1 {
		t.Fatalf("wait calls = %d, want 1", waitCalls)
	}
	if last&uint64(snowflakeMaxSequence) != 0 {
		t.Fatalf("sequence after rollover = %d, want 0", last&uint64(snowflakeMaxSequence))
	}
}

func TestSnowflakeClockRollbackFailsSafely(t *testing.T) {
	current := snowflakeEpochUnixMilli + 300
	clock := func() time.Time { return time.UnixMilli(current) }
	generator, err := NewSnowflakeWithClock(2, clock)
	if err != nil {
		t.Fatal(err)
	}
	first, err := generator.NextID()
	if err != nil {
		t.Fatal(err)
	}
	current--
	if _, err := generator.NextID(); !errors.Is(err, ErrClockMovedBackward) {
		t.Fatalf("rollback: expected error, got %v", err)
	}
	current++
	second, err := generator.NextID()
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatal("ID repeated after rollback error")
	}
}

func TestSnowflakeIDFormatsAsUnsignedDecimal(t *testing.T) {
	clock := func() time.Time {
		return time.UnixMilli(snowflakeEpochUnixMilli + (1 << 31))
	}
	generator, err := NewSnowflakeWithClock(1023, clock)
	if err != nil {
		t.Fatal(err)
	}
	id, err := generator.NextID()
	if err != nil {
		t.Fatal(err)
	}
	if id <= 1<<53 {
		t.Fatalf("ID %d did not exercise uint64-safe decimal range", id)
	}
	text := strconv.FormatUint(id, 10)
	parsed, err := strconv.ParseUint(text, 10, 64)
	if err != nil || parsed != id {
		t.Fatalf("decimal round trip failed for %d: %q (%v)", id, text, err)
	}
}
