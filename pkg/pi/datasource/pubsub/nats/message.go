package nats

import (
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

type natsMessage struct {
	msg    jetstream.Msg
	logger pubsub.Logger
}

func newNATSMessage(msg jetstream.Msg, logger pubsub.Logger) *natsMessage {
	return &natsMessage{
		msg:    msg,
		logger: logger,
	}
}

func (nmsg *natsMessage) Commit() {
	if err := nmsg.msg.Ack(); err != nil {
		nmsg.logger.Errorf("unable to acknowledge message on Client jStream: %v", err)
	}
}
