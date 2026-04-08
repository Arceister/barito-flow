package flow

import (
	"sync"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestLeakyBucket(t *testing.T) {
	max := int32(4)

	bucket := NewLeakyBucket(max)
	FatalIf(t, bucket.Max() != max, "bucket.Max() is wrong")
	FatalIf(t, bucket.Token() != max, "bucket.Token() is wrong")

	for i := int32(0); i < max; i++ {
		FatalIf(t, !bucket.Take(1), "bucket still have token")
	}

	FatalIf(t, bucket.Take(1), "bucket is empty")

	bucket.Refill()
	FatalIf(t, !bucket.IsFull(), "bucket must be full")
	FatalIf(t, !bucket.Take(1), "bucket is refilled")
}

func TestUpdateMax(t *testing.T) {
	max := int32(4)
	bucket := NewLeakyBucket(max)

	bucket.Take(2)
	bucket.UpdateMax(6)
	bucket.Take(2)
	FatalIf(t, !bucket.Take(1), "bucket is empty")
}

func TestLeakyBucket_ConcurrentTakeAndRefill(t *testing.T) {
	bucket := NewLeakyBucket(1000)

	var wg sync.WaitGroup

	// Multiple goroutines taking tokens
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				bucket.Take(1)
			}
		}()
	}

	// Concurrent refills
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				bucket.Refill()
			}
		}()
	}

	wg.Wait()
}

func TestLeakyBucket_ConcurrentTakeAndUpdateMax(t *testing.T) {
	bucket := NewLeakyBucket(100)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				bucket.Take(1)
			}
		}()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				bucket.UpdateMax(int32(100 + id))
			}
		}(i)
	}

	wg.Wait()
}

func TestLeakyBucket_ConcurrentReadersAndWriters(t *testing.T) {
	bucket := NewLeakyBucket(500)

	var wg sync.WaitGroup

	// Readers: Token, Max, IsFull
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = bucket.Token()
				_ = bucket.Max()
				_ = bucket.IsFull()
			}
		}()
	}

	// Writers: Take, Refill, UpdateMax
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				bucket.Take(1)
				bucket.Refill()
				bucket.UpdateMax(500)
			}
		}()
	}

	wg.Wait()
}

func TestLeakyBucket_RefillRestoresMax(t *testing.T) {
	bucket := NewLeakyBucket(10)
	bucket.Take(5)
	FatalIf(t, bucket.Token() != 5, "token should be 5 after taking 5")
	bucket.Refill()
	FatalIf(t, bucket.Token() != 10, "token should be restored to max after refill")
	FatalIf(t, !bucket.IsFull(), "bucket should be full after refill")
}

func TestLeakyBucket_TakeRejectsOverdraw(t *testing.T) {
	bucket := NewLeakyBucket(5)
	FatalIf(t, !bucket.Take(5), "should allow taking exactly max")
	FatalIf(t, bucket.Take(1), "should reject take when empty")
	FatalIf(t, bucket.Token() != 0, "token should be 0")
}
