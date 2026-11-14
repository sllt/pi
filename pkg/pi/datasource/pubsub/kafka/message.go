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
	var copied kafka.Message
	if msg != nil {
		copied = *msg
		if msg.Key != nil {
			copied.Key = append([]byte(nil), msg.Key...)
		}
		if msg.Value != nil {
			copied.Value = append([]byte(nil), msg.Value...)
		}
	}
	return &kafkaMessage{
		msg:    &copied,
		reader: reader,
		logger: logger,
	}
}

func (kmsg *kafkaMessage) Commit() {
	if kmsg.reader != nil && kmsg.msg != nil {
		err := kmsg.reader.CommitMessages(context.Background(), *kmsg.msg)
		if err != nil && kmsg.logger != nil {
			kmsg.logger.Errorf("unable to commit message on kafka")
		}
	}
}

func (kmsg *kafkaMessage) CommitContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if kmsg == nil || kmsg.reader == nil || kmsg.msg == nil {
		return nil
	}
	err := kmsg.reader.CommitMessages(ctx, *kmsg.msg)
	if err != nil && kmsg.logger != nil {
		kmsg.logger.Errorf("unable to commit message on kafka: %v", err)
	}
	return err
}

func (*kafkaMessage) AckSemantics() pubsub.AckSemantics {
	return pubsub.AckSemantics{
		Mode:                  pubsub.AckModeCumulativeOffset,
		SupportsContextCommit: true,
		SupportsNack:          false,
		SupportsSeek:          false,
	}
}

func (kmsg *kafkaMessage) MessageMeta() pubsub.MessageMeta {
	if kmsg == nil || kmsg.msg == nil {
		return pubsub.MessageMeta{}
	}
	var keyCopy []byte
	if len(kmsg.msg.Key) > 0 {
		keyCopy = append([]byte(nil), kmsg.msg.Key...)
	}
	return pubsub.MessageMeta{
		Partition:    kmsg.msg.Partition,
		HasPartition: true,
		Offset:       kmsg.msg.Offset,
		HasOffset:    true,
		Key:          keyCopy,
	}
}
