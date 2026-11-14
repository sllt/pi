package mqtt

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

type message struct {
	msg mqtt.Message
}

func (m *message) Commit() {
	if m != nil && m.msg != nil {
		m.msg.Ack()
	}
}

func (*message) AckSemantics() pubsub.AckSemantics {
	return pubsub.AckSemantics{
		Mode: pubsub.AckModeAuto,
	}
}
