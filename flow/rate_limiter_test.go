package flow

import (
	"sync"
	"testing"
	"time"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestRateLimiter_IsHitMax_CreateNewBucketIfNotExist(t *testing.T) {
	limiter := &rateLimiter{
		duration:  1,
		bucketMap: make(map[string]*bucketEntry),
	}

	isHit := limiter.IsHitLimit("some-topic", 1, 13)

	_, ok := limiter.bucketMap["some-topic"]
	FatalIf(t, !ok, "must be create new bucket with key some-topic")
	FatalIf(t, isHit, "new bucket must be full")
}

func TestRateLimiter(t *testing.T) {
	max := int32(5)

	limiter := NewRateLimiter(1)
	limiter.PutBucket("abc", NewLeakyBucket(max))
	limiter.PutBucket("def", NewLeakyBucket(max))
	limiter.Start()

	time.Sleep(1 * time.Millisecond)
	FatalIf(t, !limiter.IsStart(), "limiter should be start")

	for i := int32(0); i < max; i++ {
		FatalIf(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", i)
	}

	FatalIf(t, !limiter.IsHitLimit("abc", 1, max), "it should be hit limit at abc")
	FatalIf(t, limiter.IsHitLimit("def", 1, max), "it should be still have token at def")

	// wait until refill time
	time.Sleep(2 * time.Second)
	FatalIf(t, !limiter.Bucket("abc").IsFull(), "bucket must be full")
	FatalIf(t, !limiter.Bucket("def").IsFull(), "bucket must be full")

	limiter.Stop()
	time.Sleep(1 * time.Millisecond)
	FatalIf(t, limiter.IsStart(), "limiter should be stop")
}

func TestRateLimiter_Batch(t *testing.T) {
	max := int32(5)

	limiter := NewRateLimiter(1)
	limiter.PutBucket("abc", NewLeakyBucket(max))
	limiter.Start()

	time.Sleep(1 * time.Millisecond)
	FatalIf(t, !limiter.IsStart(), "limiter should be start")

	FatalIf(t, !limiter.IsHitLimit("abc", 6, max), "it should be hit limit at abc")

	// wait until refill time
	time.Sleep(1 * time.Second)
	FatalIf(t, !limiter.Bucket("abc").IsFull(), "bucket must be full")

	limiter.Stop()
	time.Sleep(1 * time.Millisecond)
	FatalIf(t, limiter.IsStart(), "limiter should be stop")
}

func TestRateLimiter_IsHitLimit_UpdateMax(t *testing.T) {
	max := int32(4)
	newMax := int32(6)
	limiter := NewRateLimiter(1)
	limiter.PutBucket("abc", NewLeakyBucket(max))
	limiter.Start()

	time.Sleep(1 * time.Second)
	FatalIf(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", 0)
	FatalIf(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", 1)
	FatalIf(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", 2)
	FatalIf(t, limiter.IsHitLimit("abc", 1, max), "it should be still have token at abc: %d", 3)

	FatalIf(t, limiter.IsHitLimit("abc", 1, newMax), "it should be still have token at abc: %d", 4)
}

func TestRateLimiter_ConcurrentIsHitLimit(t *testing.T) {
	limiter := NewRateLimiter(1)
	limiter.Start()
	defer limiter.Stop()

	var wg sync.WaitGroup
	numGoroutines := 50
	numIterations := 100

	// Hammer IsHitLimit from many goroutines with different topics
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				topic := "topic-" + string(rune('A'+id%26))
				limiter.IsHitLimit(topic, 1, 100)
			}
		}(i)
	}

	wg.Wait()
}

func TestRateLimiter_ConcurrentPutAndGet(t *testing.T) {
	limiter := NewRateLimiter(1)
	limiter.Start()
	defer limiter.Stop()

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				topic := "topic-put"
				limiter.PutBucket(topic, NewLeakyBucket(int32(10+id)))
			}
		}(i)
	}

	// Readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				limiter.Bucket("topic-put")
				limiter.IsStart()
			}
		}()
	}

	wg.Wait()
}

func TestRateLimiter_ConcurrentIsHitLimitWithRefill(t *testing.T) {
	// Use a very short tick interval so refills happen during the test
	l := &rateLimiter{
		duration:  1,
		ticker:    time.NewTicker(10 * time.Millisecond),
		stop:      make(chan int),
		bucketMap: make(map[string]*bucketEntry),
	}
	l.tick = l.ticker.C
	l.Start()
	defer l.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				l.IsHitLimit("shared-topic", 1, 1000)
			}
		}(i)
	}

	wg.Wait()
}

func TestRateLimiter_BucketEviction(t *testing.T) {
	l := &rateLimiter{
		duration:  1,
		ticker:    time.NewTicker(50 * time.Millisecond),
		stop:      make(chan int),
		bucketMap: make(map[string]*bucketEntry),
	}
	l.tick = l.ticker.C

	// Manually add a stale entry
	l.mu.Lock()
	l.bucketMap["stale-topic"] = &bucketEntry{
		bucket:     NewLeakyBucket(10),
		lastAccess: time.Now().Add(-20 * time.Minute), // 20 min ago, well past threshold
	}
	l.bucketMap["fresh-topic"] = &bucketEntry{
		bucket:     NewLeakyBucket(10),
		lastAccess: time.Now(),
	}
	l.mu.Unlock()

	// Trigger refill which should evict the stale entry
	l.refillBuckets()

	l.mu.RLock()
	_, staleExists := l.bucketMap["stale-topic"]
	_, freshExists := l.bucketMap["fresh-topic"]
	l.mu.RUnlock()

	FatalIf(t, staleExists, "stale bucket should have been evicted")
	FatalIf(t, !freshExists, "fresh bucket should still exist")

	l.ticker.Stop()
}

func TestRateLimiter_BucketEviction_AccessKeepsAlive(t *testing.T) {
	l := &rateLimiter{
		duration:  1,
		ticker:    time.NewTicker(50 * time.Millisecond),
		stop:      make(chan int),
		bucketMap: make(map[string]*bucketEntry),
	}
	l.tick = l.ticker.C

	// Create topic via IsHitLimit
	l.IsHitLimit("active-topic", 1, 100)

	l.mu.RLock()
	entry, exists := l.bucketMap["active-topic"]
	l.mu.RUnlock()

	FatalIf(t, !exists, "topic should exist after IsHitLimit")
	FatalIf(t, time.Since(entry.lastAccess) > time.Second, "lastAccess should be recent")

	l.ticker.Stop()
}

func TestRateLimiter_BucketReturnsNilForNonexistent(t *testing.T) {
	limiter := NewRateLimiter(1)
	bucket := limiter.Bucket("nonexistent")
	FatalIf(t, bucket != nil, "Bucket should return nil for nonexistent topic")
}

func TestRateLimiter_IsHitLimit_DoubleCheckLocking(t *testing.T) {
	// Test that concurrent first-access to the same topic creates only one bucket
	limiter := NewRateLimiter(1)
	limiter.Start()
	defer limiter.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			limiter.IsHitLimit("same-topic", 1, 100)
		}()
	}
	wg.Wait()

	// Should have exactly one bucket for "same-topic"
	bucket := limiter.Bucket("same-topic")
	FatalIf(t, bucket == nil, "bucket should exist for same-topic")
}
