package flow

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/BaritoLog/barito-flow/prome"
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/IBM/sarama"
	"github.com/golang/protobuf/jsonpb"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

type KafkaConsumerGCSSettings struct {
	GroupIDPrefix         string `envconfig:"kafka_group_id_prefix" required:"true"`    // consumer_group name
	TopicPattern          string `envconfig:"kafka_topic_pattern" required:"true"`      // topic pattern to be consumed, regex
	TimberToSimplerFormat bool   `envconfig:"timber_to_simpler_format" default:"false"` // use simpler format for GCS
}

type baritoKafkaConsumerGCSService struct {
	groupIDPrefix     string
	topicPatternRegex regexp.Regexp

	workerMap    map[string]types.ConsumerWorker
	gcsOutputMap map[string]types.ConsumerOutput

	kafkaAdmin            types.KafkaAdmin
	kafkaFactory          types.KafkaFactory
	consumerOuputFactory  types.ConsumerOutputFactory
	marshaler             *jsonpb.Marshaler
	timberToSimplerFormat bool

	logger *log.Entry
	done   chan struct{}
	wg     sync.WaitGroup
}

func NewBaritoKafkaConsumerGCSFromEnv(kafkaFactory types.KafkaFactory, consumerOutputFactory types.ConsumerOutputFactory) BaritoConsumerService {
	settings := KafkaConsumerGCSSettings{}
	envconfig.MustProcess("", &settings)

	kafkaAdmin, err := kafkaFactory.MakeKafkaAdmin()
	if err != nil {
		panic(fmt.Errorf("Error creating kafka admin: %s", err))
	}

	s := &baritoKafkaConsumerGCSService{
		groupIDPrefix:         settings.GroupIDPrefix,
		topicPatternRegex:     *regexp.MustCompile(settings.TopicPattern),
		kafkaAdmin:            kafkaAdmin,
		kafkaFactory:          kafkaFactory,
		consumerOuputFactory:  consumerOutputFactory,
		workerMap:             make(map[string]types.ConsumerWorker),
		gcsOutputMap:          make(map[string]types.ConsumerOutput),
		marshaler:             &jsonpb.Marshaler{},
		timberToSimplerFormat: settings.TimberToSimplerFormat,
		logger:                log.New().WithField("component", "BaritoKafkaConsumerGCS"),
		done:                  make(chan struct{}),
	}
	return s
}

func (s *baritoKafkaConsumerGCSService) Start() error {
	s.logger.Warn("Start Barito Kafka Consumer GCS Service")
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		// Run immediately on start, then on ticker
		s.refreshAndSpawnWorkers()

		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				s.refreshAndSpawnWorkers()
			}
		}
	}()

	return nil
}

func (s *baritoKafkaConsumerGCSService) refreshAndSpawnWorkers() {
	err := s.kafkaAdmin.RefreshTopics()
	if err != nil {
		prome.IncreaseConsumerCustomErrorTotal("kafka_admin_refresh_topics")
		s.logger.Error(err)
	}
	for _, topic := range s.kafkaAdmin.Topics() {
		if !s.topicPatternRegex.MatchString(topic) {
			s.logger.Debug("Topic doesn't match pattern", topic)
			continue
		}

		if _, ok := s.workerMap[topic]; ok {
			s.logger.Debug("Topic already being handled", topic)
			continue
		}
		s.logger.Warn("Handling topic ", topic)

		err := s.spawnLogsWorker(topic, sarama.OffsetOldest)
		if err != nil {
			prome.IncreaseConsumerCustomErrorTotalf("spawn_logs_worker_%s", topic)
			s.logger.WithField("topic", topic).Error(err)
			continue
		}
	}
}

func (s *baritoKafkaConsumerGCSService) spawnLogsWorker(topic string, initialOffset int64) (err error) {
	// create gcs types.ConsumerOutput
	g, err := s.consumerOuputFactory.MakeConsumerOutputGCS(topic)
	if err != nil {
		err = fmt.Errorf("Error creating gcs output: %s", err)
		s.logger.WithField("topic", topic).Error(err)
		return err
	}
	s.gcsOutputMap[topic] = g

	// consumerggroup name, is prefix + _ + topic
	groupID := s.groupIDPrefix + topic

	// create or get kafka consumer
	consumer, err := s.kafkaFactory.MakeClusterConsumer(groupID, topic, initialOffset)
	if err != nil {
		err = errkit.Concat(ErrConsumerWorker, err)
		s.logger.WithField("topic", topic).Error(err)
		return err
	}
	worker := s.kafkaFactory.MakeConsumerWorker(topic, consumer)
	s.workerMap[topic] = worker

	// log when on error
	worker.OnError(func(err error) {
		s.logger.WithField("topic", topic).Error(err)
	})

	// when there are new message, call the gcs OnMessage
	worker.OnSuccess(func(msg *sarama.ConsumerMessage) {
		timber, err := ConvertKafkaMessageToTimber(msg)
		if err != nil {
			err = errkit.Concat(ErrConvertKafkaMessage, err)
			s.logger.WithField("topic", topic).Error(err)
			return
		}

		var content string
		if s.timberToSimplerFormat {
			content, err = ConvertTimberToLogFormatGCSSimpleString(timber)
		} else {
			content, err = s.marshaler.MarshalToString(timber.GetContent())
		}
		if err != nil {
			s.logger.WithField("topic", topic).Error(err)
			return
		}

		// retry indefinitely until success
		for {
			err := g.OnMessage([]byte(content))
			if err == nil {
				break
			}
			prome.IncreaseConsumerCustomErrorTotalf("gcs_on_message: %s", topic)
			time.Sleep(1 * time.Second)
		}
	})

	// setup hook when the GCSService flush, it will commit the offset to the kafka
	g.AddOnFlushFunc(func() error {
		err := worker.OnConsumerFlush()
		return err
	})

	worker.Start()
	go g.Start()

	return
}

func (s *baritoKafkaConsumerGCSService) Close() {
	close(s.done)
	s.wg.Wait()

	for _, g := range s.gcsOutputMap {
		g.Stop()
	}

	for _, w := range s.workerMap {
		w.Stop()
	}
}

func (s *baritoKafkaConsumerGCSService) WorkerMap() map[string]types.ConsumerWorker {
	return s.workerMap
}

func (s *baritoKafkaConsumerGCSService) NewTopicEventWorker() types.ConsumerWorker {
	// irrelevant, this consumer didn't use new_topic_event
	return nil
}

