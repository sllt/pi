package pubsub

import "context"

// AckMode describes how a message broker acknowledges delivered messages.
type AckMode uint8

const (
	// AckModeUnknown indicates the adapter has not declared acknowledgment semantics.
	// Legacy Subscribe remains supported, while strict policies (such as SubscriberDrain
	// or cumulative-offset guarantees) require explicit capability evidence.
	AckModeUnknown AckMode = iota
	// AckModeAuto indicates fire-and-forget or broker/SDK auto-ack on delivery
	// (e.g. Redis Pub/Sub channels or MQTT auto-ack).
	AckModeAuto
	// AckModeIndependent indicates per-message acknowledgment by ID/handle
	// (e.g. Redis Streams XACK or Google Cloud Pub/Sub Ack).
	AckModeIndependent
	// AckModeCumulativeOffset indicates partition-ordered cumulative offset commits
	// where committing offset K acknowledges all offsets <= K on that partition (e.g. Kafka).
	AckModeCumulativeOffset
)

func (m AckMode) String() string {
	switch m {
	case AckModeAuto:
		return "auto"
	case AckModeIndependent:
		return "independent"
	case AckModeCumulativeOffset:
		return "cumulative_offset"
	default:
		return "unknown"
	}
}

// AckSemantics describes the acknowledgment and recovery capabilities of a Subscriber or Committer.
type AckSemantics struct {
	Mode                  AckMode
	SupportsContextCommit bool
	SupportsNack          bool
	SupportsSeek          bool
}

// AckSemanticsProvider is an optional interface implemented by pubsub adapters
// and/or message committers to declare acknowledgment semantics.
type AckSemanticsProvider interface {
	AckSemantics() AckSemantics
}

// ContextCommitter is an optional extension to Committer that respects context
// cancellation/deadlines and returns any underlying broker commit error.
type ContextCommitter interface {
	CommitContext(ctx context.Context) error
}

// Nacker is an optional extension implemented by message committers that can
// explicitly negatively acknowledge a message for redelivery.
type Nacker interface {
	Nack()
}

// MessageMeta carries broker-supplied partition, offset, message ID, and key metadata.
type MessageMeta struct {
	Partition    int
	HasPartition bool
	Offset       int64
	HasOffset    bool
	MessageID    string
	Key          []byte
}

// MessageMetaProvider is an optional interface implemented by message committers
// (or Message.MetaData values) to expose broker partition/offset metadata.
type MessageMetaProvider interface {
	MessageMeta() MessageMeta
}

// ResolveAckSemantics inspects a Committer first and falls back to the Subscriber.
func ResolveAckSemantics(sub Subscriber, committer Committer) AckSemantics {
	if committer != nil {
		if p, ok := committer.(AckSemanticsProvider); ok {
			sem := p.AckSemantics()
			if _, hasCtxCommit := committer.(ContextCommitter); hasCtxCommit {
				sem.SupportsContextCommit = true
			}
			if _, hasNack := committer.(Nacker); hasNack {
				sem.SupportsNack = true
			}
			return sem
		}
	}
	var sem AckSemantics
	if sub != nil {
		if p, ok := sub.(AckSemanticsProvider); ok {
			sem = p.AckSemantics()
		}
	}
	if committer != nil {
		if _, hasCtxCommit := committer.(ContextCommitter); hasCtxCommit {
			sem.SupportsContextCommit = true
		}
		if _, hasNack := committer.(Nacker); hasNack {
			sem.SupportsNack = true
		}
	}
	return sem
}

// ResolveMessageMeta extracts broker metadata from a Message's Committer or MetaData.
func ResolveMessageMeta(msg *Message) MessageMeta {
	if msg == nil {
		return MessageMeta{}
	}
	if msg.Committer != nil {
		if p, ok := msg.Committer.(MessageMetaProvider); ok {
			return p.MessageMeta()
		}
	}
	if msg.MetaData != nil {
		if p, ok := msg.MetaData.(MessageMetaProvider); ok {
			return p.MessageMeta()
		}
		if metaVal, ok := msg.MetaData.(MessageMeta); ok {
			return metaVal
		}
	}
	return MessageMeta{}
}
