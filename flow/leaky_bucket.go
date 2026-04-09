package flow

import (
	"sync"
)

type LeakyBucket struct {
	max   int32
	token int32
	lock  sync.Mutex
}

func NewLeakyBucket(max int32) *LeakyBucket {
	return &LeakyBucket{
		max:   max,
		token: max,
	}
}

func (b *LeakyBucket) Token() int32 {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.token
}

func (b *LeakyBucket) Max() int32 {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.max
}

func (b *LeakyBucket) UpdateMax(newMax int32) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if newMax > b.max {
		b.token = b.token + (newMax - b.max)
	}
	b.max = newMax
}

func (b *LeakyBucket) IsFull() bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.token == b.max
}

func (l *LeakyBucket) Refill() {
	l.lock.Lock()
	l.token = l.max
	l.lock.Unlock()
}

func (l *LeakyBucket) Take(count int) bool {
	l.lock.Lock()
	defer l.lock.Unlock()
	if (l.token - int32(count)) < 0 {
		return false
	}
	l.token = l.token - int32(count)
	return true
}
