package systree

import (
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/types"
)

type Topics interface {
	Publish(*message.PublishMessage) error
	Retain(types.RetainObject) error
}

// Provider systree provider
type Provider interface {
	SetCallbacks(Topics)
	Metric() Metric
	Topics() TopicsStat
	Subscriptions() SubscriptionsStat
	Clients() Clients
	Sessions() SessionsStat
}

// Metric is wrap around all of metrics
type Metric interface {
	Bytes() BytesMetric
	Packets() PacketsMetric
}

// PacketsMetric packets metric
type PacketsMetric interface {
	Sent(t message.PacketType)
	Received(t message.PacketType)
}

// BytesMetric bytes metric
type BytesMetric interface {
	Sent(bytes uint64)
	Received(bytes uint64)
}

// SessionsStat Statistic of sessions
type SessionsStat interface {
	Created()
	Removed()
}

// Clients Statistic of sessions
type Clients interface {
	Connected(string, *ClientConnectStatus)
	Disconnected(string, message.ReasonCode, bool)
}

// TopicsStat statistic of topics
type TopicsStat interface {
	Added()
	Removed()
}

type SubscriptionsStat interface {
	Subscribed()
	UnSubscribed()
}

type TreeRetainMessage interface {
	Publish() *message.PublishMessage
}
