package cmds

import (
	"testing"

	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/barito-flow/prome"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.ErrorLevel)
	prome.InitProducerInstrumentation()
}

func TestProducer_KafkaError(t *testing.T) {
	factory := flow.NewDummyKafkaFactory()
	factory.Expect_MakeSyncProducerFunc_AlwaysError("some-error")

	limiter := flow.NewDummyRateLimiter()

	producerParams := map[string]interface{}{
		"factory":                factory,
		"grpcAddr":               ":24400",
		"restAddr":               ":8080",
		"rateLimitResetInterval": 1,
		"topicSuffix":            "_logs",
		"topicPrefix":            "",
		"kafkaMaxRetry":          1,
		"kafkaRetryInterval":     1,
		"newEventTopic":          "new_topic_events",
		"grpcMaxRecvMsgSize":     20000000,
		"ignoreKafkaOptions":     false,
		"kafkaMessageFormat":     "proto",
		"limiter":                limiter,
	}

	service := flow.NewProducerService(producerParams)
	err := service.Start()

	FatalIfWrongError(t, err, "Make sync producer failed")
}
