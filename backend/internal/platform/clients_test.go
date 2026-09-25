package platform

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testRedisAtomic(t *testing.T) (*redis.Client, redisAdapter, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR is not configured; start the project Redis to run integration tests")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		t.Fatalf("Redis is unavailable at %s: %v", addr, err)
	}
	keyPrefix := "test:atomic:" + strings.ReplaceAll(t.Name(), "/", "_") + ":" + randomKeySuffix(t) + ":"
	t.Cleanup(func() {
		iter := c.Scan(context.Background(), 0, keyPrefix+"*", 0).Iterator()
		var keys []string
		for iter.Next(context.Background()) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			_ = c.Del(context.Background(), keys...).Err()
		}
		_ = c.Close()
	})
	return c, redisAdapter{client: c}, keyPrefix
}

func randomKeySuffix(t *testing.T) string {
	t.Helper()
	var b [12]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		t.Fatalf("generate Redis key suffix: %v", err)
	}
	return hex.EncodeToString(b[:])
}

func TestRedisAtomicSessionReplacementAndDelayedDelete(t *testing.T) {
	c, r, keyPrefix := testRedisAtomic(t)
	ctx := context.Background()
	prefix := keyPrefix + "session:"
	slotPC, slotMobile := prefix+"slot:pc", prefix+"slot:mobile"
	now := time.Now().Unix()
	record := RedisSessionRecord{UserID: 42, DeviceType: "pc", Role: "user", Permissions: []string{"buy"}, RefreshHash: "hash-1", CreatedAt: now, ExpiresAt: time.Now().Add(3 * time.Second).Unix()}
	if old, err := r.ReplaceSession(ctx, RedisSessionKeys{SlotKey: slotPC, SessionKeyPrefix: prefix, SID: "sid-1", SessionKey: prefix + "sid-1", RefreshIndexKey: keyPrefix + "refresh:hash-1"}, record, 3*time.Second); err != nil || old != "" {
		t.Fatalf("first replacement old=%q err=%v", old, err)
	}
	if old, err := r.ReplaceSession(ctx, RedisSessionKeys{SlotKey: slotMobile, SessionKeyPrefix: prefix, SID: "mobile-1", SessionKey: prefix + "mobile-1", RefreshIndexKey: keyPrefix + "refresh:m"}, RedisSessionRecord{UserID: 42, DeviceType: "mobile", Role: "user", RefreshHash: "m", CreatedAt: now, ExpiresAt: time.Now().Add(3 * time.Second).Unix()}, 3*time.Second); err != nil || old != "" {
		t.Fatalf("mobile replacement old=%q err=%v", old, err)
	}
	if old, err := r.ReplaceSession(ctx, RedisSessionKeys{SlotKey: slotPC, SessionKeyPrefix: prefix, SID: "sid-2", SessionKey: prefix + "sid-2", RefreshIndexKey: keyPrefix + "refresh:hash-2"}, RedisSessionRecord{UserID: 42, DeviceType: "pc", Role: "user", RefreshHash: "hash-2", CreatedAt: now, ExpiresAt: time.Now().Add(3 * time.Second).Unix()}, 3*time.Second); err != nil || old != "sid-1" {
		t.Fatalf("replacement old=%q err=%v", old, err)
	}
	if got, _ := c.Get(ctx, slotMobile).Result(); got != "mobile-1" {
		t.Fatalf("mobile slot changed: %q", got)
	}
	if exists, _ := c.Exists(ctx, prefix+"sid-1").Result(); exists != 0 {
		t.Fatal("old session was not removed")
	}
	deleted, err := r.DeleteSessionIfCurrent(ctx, RedisSessionKeys{SlotKey: slotPC, SessionKey: prefix + "sid-1", SID: "sid-1"})
	if err != nil || deleted {
		t.Fatalf("stale delete removed current slot: deleted=%v err=%v", deleted, err)
	}
	if got, _ := c.Get(ctx, slotPC).Result(); got != "sid-2" {
		t.Fatalf("current slot changed after stale delete: %q", got)
	}
	if p1, p2 := c.PTTL(ctx, slotPC).Val(), c.PTTL(ctx, prefix+"sid-2").Val(); p1 <= 0 || p2 <= 0 || absDuration(p1-p2) > 100*time.Millisecond {
		t.Fatalf("slot/session TTL mismatch: %v %v", p1, p2)
	}
}

func TestRedisAtomicRefreshRotationConcurrentReplay(t *testing.T) {
	c, r, keyPrefix := testRedisAtomic(t)
	ctx := context.Background()
	prefix, sid := keyPrefix+"session:", "sid-refresh"
	if _, err := r.ReplaceSession(ctx, RedisSessionKeys{SlotKey: prefix + "slot", SessionKeyPrefix: prefix, SID: sid, SessionKey: prefix + sid, RefreshIndexKey: prefix + "refresh:old"}, RedisSessionRecord{UserID: 1, DeviceType: "pc", Role: "user", RefreshHash: "old", CreatedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Minute).Unix()}, time.Minute); err != nil {
		t.Fatal(err)
	}
	const n = 16
	results := make(chan RefreshRotationResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _, err := r.RotateRefreshHash(ctx, RedisRefreshKeys{SessionKey: prefix + sid, ReplayKey: prefix + "replay:old", NewRefreshIndex: prefix + "refresh:new", SID: sid}, "old", "new", time.Minute)
			if err != nil {
				t.Errorf("rotate: %v", err)
				return
			}
			results <- status
		}()
	}
	wg.Wait()
	close(results)
	var success, replay int
	for status := range results {
		if status == RefreshRotationSucceeded {
			success++
		}
		if status == RefreshRotationReplay {
			replay++
		}
	}
	if success != 1 || replay != n-1 {
		t.Fatalf("rotation results success=%d replay=%d", success, replay)
	}
	status, replaySID, err := r.RotateRefreshHash(ctx, RedisRefreshKeys{SessionKey: prefix + sid, ReplayKey: prefix + "replay:old", NewRefreshIndex: prefix + "refresh:other", SID: sid}, "old", "other", time.Minute)
	if err != nil || status != RefreshRotationReplay || replaySID != sid {
		t.Fatalf("replay status=%v sid=%q err=%v", status, replaySID, err)
	}
	if ttl := c.PTTL(ctx, prefix+"replay:old").Val(); ttl <= 0 {
		t.Fatalf("replay marker has no TTL: %v", ttl)
	}
}

func TestRedisAtomicIncrementWithTTLAndFailure(t *testing.T) {
	c, r, keyPrefix := testRedisAtomic(t)
	ctx := context.Background()
	key := keyPrefix + "counter"
	count, err := r.IncrementWithTTL(ctx, key, time.Minute)
	if err != nil || count != 1 {
		t.Fatalf("first increment count=%d err=%v", count, err)
	}
	if ttl := c.PTTL(ctx, key).Val(); ttl <= 0 {
		t.Fatalf("counter has no TTL: %v", ttl)
	}
	count, err = r.IncrementWithTTL(ctx, key, time.Minute)
	if err != nil || count != 2 {
		t.Fatalf("second increment count=%d err=%v", count, err)
	}
	closed := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer closed.Close()
	if _, err := (redisAdapter{client: closed}).IncrementWithTTL(ctx, "test:failure", time.Minute); err == nil {
		t.Fatal("Redis failure was swallowed")
	}
}

func TestRedisAtomicReplaceSessionValidation(t *testing.T) {
	ttl := time.Minute
	record := RedisSessionRecord{UserID: 1, DeviceType: "pc", Role: "user", RefreshHash: "hash", CreatedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(ttl).Unix()}
	valid := RedisSessionKeys{SlotKey: "slot", SessionKeyPrefix: "session:", SID: "sid", SessionKey: "session:sid", RefreshIndexKey: "refresh:hash"}

	for name, keys := range map[string]RedisSessionKeys{
		"zero user ID":   valid,
		"mismatched key": valid,
	} {
		caseRecord := record
		switch name {
		case "zero user ID":
			caseRecord.UserID = 0
		case "mismatched key":
			keys.SessionKey = "session:other"
		}
		t.Run(name, func(t *testing.T) {
			if _, err := (redisAdapter{}).ReplaceSession(context.Background(), keys, caseRecord, ttl); err == nil {
				t.Fatal("expected invalid session to be rejected")
			}
		})
	}
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
