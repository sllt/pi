package google

import (
	gcPubSub "cloud.google.com/go/pubsub"

	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

type googleMessage struct {
	msg *gcPubSub.Message
}

func newGoogleMessage(msg *gcPubSub.Message) *googleMessage {
	return &googleMessage{msg: msg}
}

func (gm *googleMessage) Commit() {
	if gm != nil && gm.msg != nil {
		gm.msg.Ack()
	}
}

func (gm *googleMessage) Nack() {
	if gm != nil && gm.msg != nil {
		gm.msg.Nack()
	}
}

func (*googleMessage) AckSemantics() pubsub.AckSemantics {
	return pubsub.AckSemantics{
		Mode:                  pubsub.AckModeIndependent,
		SupportsContextCommit: false,
		SupportsNack:          true,
	}
}

func (gm *googleMessage) MessageMeta() pubsub.MessageMeta {
	if gm == nil || gm.msg == nil {
		return pubsub.MessageMeta{}
	}
	var key []byte
	if gm.msg.OrderingKey != "" {
		key = []byte(gm.msg.OrderingKey)
	}
	return pubsub.MessageMeta{
		MessageID: gm.msg.ID,
		Key:       key,
	}
}
