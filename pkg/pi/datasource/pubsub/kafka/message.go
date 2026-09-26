package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"

	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

type kafkaMessage struct {
	msg    *kafka.Message
	reader Reader
	logger pubsub.Logger
}

func newKafkaMessage(msg *kafka.Message, reader Reader, logger pubsub.Logger) *kafkaMessage {
	return &kafkaMessage{
		msg:    msg,
		reader: reader,
		logger: logger,
	}
}

func (kmsg *kafkaMessage) Commit() {
	if kmsg.reader != nil {
		err := kmsg.reader.CommitMessages(context.Background(), *kmsg.msg)
		if err != nil {
			kmsg.logger.Errorf("unable to commit message on kafka")
		}
	}
}
