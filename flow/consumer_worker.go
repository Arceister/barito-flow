package flow

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/BaritoLog/barito-flow/prome"
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

const (
	RetrieveMessageFailedError = errkit.Error("Retrieve message failed")
)

type consumerWorker struct {
	name               string
	isStart            atomic.Bool
	consumer           types.ClusterConsumer
	onErrorFunc        func(error)
	onSuccessFunc      func(*sarama.ConsumerMessage)
	onNotificationFunc func(*types.Notification)
	stop               chan struct{}
	wg                 sync.WaitGroup
	once               sync.Once
	lastMessage        *sarama.ConsumerMessage
}

func NewConsumerWorker(name string, consumer types.ClusterConsumer) types.ConsumerWorker {
	return &consumerWorker{
		name:     name,
		consumer: consumer,
		stop:     make(chan struct{}),
	}
}

func (w *consumerWorker) Start() {
	log.Warnf("Start worker '%s'", w.name)

	w.wg.Add(3)
	go w.loopErrors()
	go w.loopNotification()
	go w.loopMain()
}

func (w *consumerWorker) Stop() {
	w.once.Do(func() {
		close(w.stop)
	})
	if w.consumer != nil {
		w.consumer.Close()
	}
}

func (w *consumerWorker) Halt() {
	w.once.Do(func() {
		close(w.stop)
	})
	log.Warnf("Halt worker '%s'", w.name)
}

func (w *consumerWorker) IsStart() bool {
	return w.isStart.Load()
}

func (w *consumerWorker) OnError(f func(error)) {
	w.onErrorFunc = f
}

func (w *consumerWorker) OnSuccess(f func(*sarama.ConsumerMessage)) {
	w.onSuccessFunc = f
}

func (w *consumerWorker) OnNotification(f func(*types.Notification)) {
	w.onNotificationFunc = f
}

func (w *consumerWorker) OnConsumerFlush() error {
	log.Warn("OnConsumerFlush")
	err := w.consumer.CommitOffsets()
	if err != nil {
		log.Error(fmt.Errorf("Commit offset failed: %s", err))
	}
	return err
}

func (w *consumerWorker) loopMain() {
	defer w.wg.Done()
	w.isStart.Store(true)
	for {
		select {
		case message, ok := <-w.consumer.Messages():
			if ok {
				prome.IncreaseKafkaMessagesIncoming(message.Topic)
				w.fireSuccess(message)
				w.consumer.MarkOffset(message, "")
			}
		case <-w.stop:
			w.isStart.Store(false)
			return
		}
	}
}

func (w *consumerWorker) loopNotification() {
	defer w.wg.Done()
	ch := w.consumer.Notifications()
	for {
		select {
		case notification, ok := <-ch:
			if !ok {
				return
			}
			w.fireNotification(notification)
		case <-w.stop:
			return
		}
	}
}

func (w *consumerWorker) loopErrors() {
	defer w.wg.Done()
	ch := w.consumer.Errors()
	for {
		select {
		case err, ok := <-ch:
			if !ok {
				return
			}
			w.fireError(errkit.Concat(RetrieveMessageFailedError, err))
		case <-w.stop:
			return
		}
	}
}

func (w *consumerWorker) fireSuccess(message *sarama.ConsumerMessage) {
	if w.onSuccessFunc != nil {
		w.onSuccessFunc(message)
	}
}

func (w *consumerWorker) fireError(err error) {
	if w.onErrorFunc != nil {
		w.onErrorFunc(err)
	}
}

func (w *consumerWorker) fireNotification(notification *types.Notification) {
	if w.onNotificationFunc != nil {
		w.onNotificationFunc(notification)
	}
}
