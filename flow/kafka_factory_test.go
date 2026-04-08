package flow

import (
	"context"
	"testing"
	"time"

	"github.com/BaritoLog/barito-flow/flow/types"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/IBM/sarama"
)

// fakeConsumerGroup implements sarama.ConsumerGroup for testing
type fakeConsumerGroup struct {
	consumeCalled chan struct{}
	closeCalled   chan struct{}
	closed        bool
}

func newFakeConsumerGroup() *fakeConsumerGroup {
	return &fakeConsumerGroup{
		consumeCalled: make(chan struct{}, 10),
		closeCalled:   make(chan struct{}, 1),
	}
}

func (f *fakeConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	f.consumeCalled <- struct{}{}
	// Block until context is cancelled (simulates real Consume behavior)
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeConsumerGroup) Errors() <-chan error {
	return make(chan error)
}

func (f *fakeConsumerGroup) Close() error {
	f.closed = true
	f.closeCalled <- struct{}{}
	return nil
}

func (f *fakeConsumerGroup) Pause(partitions map[string][]int32)  {}
func (f *fakeConsumerGroup) Resume(partitions map[string][]int32) {}
func (f *fakeConsumerGroup) PauseAll()                            {}
func (f *fakeConsumerGroup) ResumeAll()                           {}

func TestConsumerGroupAdapter_CloseStopsConsumeGoroutine(t *testing.T) {
	group := newFakeConsumerGroup()
	adapter := newConsumerGroupAdapter(group, []string{"test-topic"})

	go adapter.consume(adapter.ctx)

	// Wait for consume to be called
	select {
	case <-group.consumeCalled:
	case <-time.After(time.Second):
		t.Fatal("consume was not called within timeout")
	}

	// Close should cancel context and stop the consume loop
	err := adapter.Close()
	FatalIf(t, err != nil, "Close should not return error")
	FatalIf(t, !group.closed, "underlying group should be closed")
}

func TestConsumerGroupAdapter_CloseClosesChannels(t *testing.T) {
	group := newFakeConsumerGroup()
	adapter := newConsumerGroupAdapter(group, []string{"test-topic"})

	go adapter.consume(adapter.ctx)

	// Wait for consume to start
	select {
	case <-group.consumeCalled:
	case <-time.After(time.Second):
		t.Fatal("consume was not called within timeout")
	}

	adapter.Close()

	// Verify messages channel is closed (range should exit)
	_, ok := <-adapter.Messages()
	FatalIf(t, ok, "messages channel should be closed")

	// Verify notifications channel is closed
	_, ok = <-adapter.Notifications()
	FatalIf(t, ok, "notifications channel should be closed")

	// Verify errors channel is closed
	_, ok = <-adapter.Errors()
	FatalIf(t, ok, "errors channel should be closed")
}

func TestConsumerGroupAdapter_ContextCancellation(t *testing.T) {
	group := newFakeConsumerGroup()
	adapter := newConsumerGroupAdapter(group, []string{"test-topic"})

	go adapter.consume(adapter.ctx)

	// Wait for consume to start
	select {
	case <-group.consumeCalled:
	case <-time.After(time.Second):
		t.Fatal("consume was not called within timeout")
	}

	// Verify context is not cancelled initially
	FatalIf(t, adapter.ctx.Err() != nil, "context should not be cancelled initially")

	// Cancel via Close
	adapter.Close()

	// Verify context is cancelled
	FatalIf(t, adapter.ctx.Err() == nil, "context should be cancelled after Close")
}

func TestConsumerGroupAdapter_ConsumeExitsOnContextCancel(t *testing.T) {
	group := newFakeConsumerGroup()
	adapter := newConsumerGroupAdapter(group, []string{"test-topic"})

	done := make(chan struct{})
	go func() {
		adapter.consume(adapter.ctx)
		close(done)
	}()

	// Wait for consume to be called at least once
	select {
	case <-group.consumeCalled:
	case <-time.After(time.Second):
		t.Fatal("consume was not called within timeout")
	}

	// Cancel the context
	adapter.cancel()

	// Consume loop should exit
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consume goroutine did not exit after context cancellation")
	}
}

func TestConsumerGroupAdapter_LoopNotificationExitsOnClose(t *testing.T) {
	group := newFakeConsumerGroup()
	adapter := newConsumerGroupAdapter(group, []string{"test-topic"})

	go adapter.consume(adapter.ctx)

	// Simulate loopNotification: range over notifications
	done := make(chan struct{})
	go func() {
		for range adapter.Notifications() {
		}
		close(done)
	}()

	// Send a notification to verify channel is working
	select {
	case adapter.notifications <- &types.Notification{Type: "test"}:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("could not send notification")
	}

	// Close should close the channel, causing range to exit
	adapter.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loopNotification goroutine did not exit after Close")
	}
}

func TestConsumerGroupAdapter_LoopErrorsExitsOnClose(t *testing.T) {
	group := newFakeConsumerGroup()
	adapter := newConsumerGroupAdapter(group, []string{"test-topic"})

	go adapter.consume(adapter.ctx)

	select {
	case <-group.consumeCalled:
	case <-time.After(time.Second):
		t.Fatal("consume was not called within timeout")
	}

	// Simulate loopErrors
	done := make(chan struct{})
	go func() {
		for range adapter.Errors() {
		}
		close(done)
	}()

	adapter.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loopErrors goroutine did not exit after Close")
	}
}

func TestConsumerGroupAdapter_MessagesExitsOnClose(t *testing.T) {
	group := newFakeConsumerGroup()
	adapter := newConsumerGroupAdapter(group, []string{"test-topic"})

	go adapter.consume(adapter.ctx)

	select {
	case <-group.consumeCalled:
	case <-time.After(time.Second):
		t.Fatal("consume was not called within timeout")
	}

	// Simulate loopMain reading messages
	done := make(chan struct{})
	go func() {
		for range adapter.Messages() {
		}
		close(done)
	}()

	adapter.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("messages reader goroutine did not exit after Close")
	}
}

func TestConsumerGroupAdapter_MarkOffset(t *testing.T) {
	group := newFakeConsumerGroup()
	adapter := newConsumerGroupAdapter(group, []string{"test-topic"})

	// MarkOffset with nil session should not panic
	msg := &sarama.ConsumerMessage{Topic: "test", Partition: 0, Offset: 1}
	adapter.MarkOffset(msg, "")
}

func TestConsumerGroupAdapter_CommitOffsetsNilSession(t *testing.T) {
	group := newFakeConsumerGroup()
	adapter := newConsumerGroupAdapter(group, []string{"test-topic"})

	// CommitOffsets with nil session should not panic
	err := adapter.CommitOffsets()
	FatalIf(t, err != nil, "CommitOffsets should not error with nil session")
}
