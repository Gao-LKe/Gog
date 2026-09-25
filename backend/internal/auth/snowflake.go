package auth

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	snowflakeNodeBits      = 10
	snowflakeSequenceBits  = 12
	snowflakeMaxNodeID     = int64(1<<snowflakeNodeBits - 1)
	snowflakeMaxSequence   = uint16(1<<snowflakeSequenceBits - 1)
	snowflakeTimestampBits = 41

	// The epoch is intentionally fixed so IDs remain sortable across generator
	// instances. Uniqueness after a process restart for the same node in the
	// same millisecond requires deployment coordination or future persistence of
	// generator state.
	snowflakeEpochUnixMilli int64 = 1577836800000 // 2020-01-01T00:00:00Z
)

var (
	ErrInvalidSnowflakeNode = errors.New("snowflake node id is out of range")
	ErrClockMovedBackward   = errors.New("snowflake clock moved backward")
	ErrClockBeforeEpoch     = errors.New("snowflake clock is before epoch")
	ErrSnowflakeOverflow    = errors.New("snowflake timestamp overflow")
)

// SnowflakeClock supplies the current time to a Generator. It is a function
// type to keep clock injection small and straightforward in tests.
type SnowflakeClock func() time.Time

// SnowflakeWaiter is called while waiting for the next millisecond after the
// per-millisecond sequence is exhausted.
type SnowflakeWaiter func(time.Duration)

// Snowflake generates positive, sortable IDs suitable for BIGINT UNSIGNED.
// A generator must not be copied after first use.
type Snowflake struct {
	mu       sync.Mutex
	nodeID   int64
	now      SnowflakeClock
	wait     SnowflakeWaiter
	lastTime int64
	sequence uint16
}

// NewSnowflake creates a generator using the system clock.
func NewSnowflake(nodeID int64) (*Snowflake, error) {
	generator, err := NewSnowflakeWithClock(nodeID, time.Now)
	if err != nil {
		return nil, err
	}
	// A process that restarts with the same configured node must not issue an
	// ID in its startup millisecond: the predecessor could have used that
	// timestamp with the same node and sequence. Force the first generated ID
	// into a later millisecond. Concurrent processes still require distinct
	// configured node IDs.
	generator.lastTime = time.Now().UnixMilli()
	generator.sequence = snowflakeMaxSequence
	return generator, nil
}

// NewSnowflakeWithClock creates a generator with an injectable clock.
func NewSnowflakeWithClock(nodeID int64, now SnowflakeClock) (*Snowflake, error) {
	return NewSnowflakeWithClockAndWaiter(nodeID, now, time.Sleep)
}

// NewSnowflakeWithClockAndWaiter is useful for deterministic tests that need
// to advance a fake clock while a sequence is exhausted.
func NewSnowflakeWithClockAndWaiter(nodeID int64, now SnowflakeClock, wait SnowflakeWaiter) (*Snowflake, error) {
	if nodeID < 0 || nodeID > snowflakeMaxNodeID {
		return nil, fmt.Errorf("%w: %d (must be between 0 and %d)", ErrInvalidSnowflakeNode, nodeID, snowflakeMaxNodeID)
	}
	if now == nil {
		return nil, errors.New("snowflake clock must not be nil")
	}
	if wait == nil {
		return nil, errors.New("snowflake waiter must not be nil")
	}
	return &Snowflake{nodeID: nodeID, now: now, wait: wait}, nil
}

// NextID returns the next ID. A clock rollback is reported and does not alter
// generator state, so callers can safely stop issuing IDs until time recovers.
func (s *Snowflake) NextID() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UnixMilli()
	if now < s.lastTime {
		return 0, fmt.Errorf("%w: %dms behind", ErrClockMovedBackward, s.lastTime-now)
	}
	if now < snowflakeEpochUnixMilli {
		return 0, ErrClockBeforeEpoch
	}

	if now == s.lastTime {
		if s.sequence == snowflakeMaxSequence {
			for now <= s.lastTime {
				s.wait(time.Millisecond)
				now = s.now().UnixMilli()
				if now < s.lastTime {
					return 0, fmt.Errorf("%w: %dms behind", ErrClockMovedBackward, s.lastTime-now)
				}
			}
			s.sequence = 0
		} else {
			s.sequence++
		}
	} else {
		s.sequence = 0
	}

	delta := now - snowflakeEpochUnixMilli
	if delta >= 1<<snowflakeTimestampBits {
		return 0, ErrSnowflakeOverflow
	}
	s.lastTime = now
	// Reserve zero: at the exact epoch, node 0 and sequence 0 would otherwise
	// encode to zero. Advance the stored sequence as well so the next ID cannot
	// repeat the value returned here.
	if delta == 0 && s.nodeID == 0 && s.sequence == 0 {
		s.sequence = 1
	}
	return uint64(delta)<<(snowflakeNodeBits+snowflakeSequenceBits) |
		uint64(s.nodeID<<snowflakeSequenceBits) | uint64(s.sequence), nil
}

// Generate is a short alias for NextID.
func (s *Snowflake) Generate() (uint64, error) { return s.NextID() }
