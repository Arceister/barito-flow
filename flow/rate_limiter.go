package flow

import (
	"sync"
	"time"
)

const bucketEvictionThreshold = 10 * time.Minute

type Limiter interface {
	IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool
}

type LimiterFunc func(topic string, count int, maxTokenIfNotExist int32) bool

func (fn LimiterFunc) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	return fn(topic, count, maxTokenIfNotExist)
}

type RateLimiter interface {
	Limiter
	Start()
	Stop()
	IsStart() bool
	PutBucket(topic string, bucket *LeakyBucket)
	Bucket(topic string) *LeakyBucket
}

type bucketEntry struct {
	bucket     *LeakyBucket
	lastAccess time.Time
}

type rateLimiter struct {
	mu        sync.RWMutex
	isStart   bool
	duration  int32
	ticker    *time.Ticker
	tick      <-chan time.Time
	stop      chan int
	bucketMap map[string]*bucketEntry
}

func NewRateLimiter(duration int) RateLimiter {
	t := time.NewTicker(time.Duration(duration) * time.Second)
	return &rateLimiter{
		duration:  int32(duration),
		ticker:    t,
		tick:      t.C,
		stop:      make(chan int),
		bucketMap: make(map[string]*bucketEntry),
	}
}

func (l *rateLimiter) IsHitLimit(topic string, count int, maxTokenIfNotExist int32) bool {
	l.mu.Lock()
	entry, ok := l.bucketMap[topic]
	if !ok {
		entry = &bucketEntry{
			bucket:     NewLeakyBucket(maxTokenIfNotExist * l.duration),
			lastAccess: time.Now(),
		}
		l.bucketMap[topic] = entry
	}
	entry.lastAccess = time.Now()
	bucket := entry.bucket
	l.mu.Unlock()

	if bucket.Max() != (maxTokenIfNotExist * l.duration) {
		bucket.UpdateMax(maxTokenIfNotExist * l.duration)
	}
	return !bucket.Take(count)
}

func (l *rateLimiter) Start() {
	go l.loopRefillBuckets()
}

func (l *rateLimiter) Stop() {
	l.ticker.Stop()
	go func() {
		l.stop <- 1
	}()
}

func (l *rateLimiter) IsStart() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.isStart
}

func (l *rateLimiter) PutBucket(topic string, bucket *LeakyBucket) {
	l.mu.Lock()
	l.bucketMap[topic] = &bucketEntry{
		bucket:     bucket,
		lastAccess: time.Now(),
	}
	l.mu.Unlock()
}

func (l *rateLimiter) Bucket(topic string) *LeakyBucket {
	l.mu.RLock()
	defer l.mu.RUnlock()
	entry, ok := l.bucketMap[topic]
	if !ok {
		return nil
	}
	return entry.bucket
}

func (l *rateLimiter) loopRefillBuckets() {
	l.mu.Lock()
	l.isStart = true
	l.mu.Unlock()
	for {
		select {
		case <-l.tick:
			l.refillBuckets()
		case <-l.stop:
			l.mu.Lock()
			l.isStart = false
			l.mu.Unlock()
			return
		}
	}
}

func (l *rateLimiter) refillBuckets() {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	for key, entry := range l.bucketMap {
		if now.Sub(entry.lastAccess) > bucketEvictionThreshold {
			delete(l.bucketMap, key)
			continue
		}
		entry.bucket.Refill()
	}
}
