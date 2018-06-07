package systree

import (
	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/volantmq/types"
)

// Provider systree provider
type Provider interface {
	SetCallbacks(types.TopicMessenger)
	Metric() Metric
	Topics() TopicsStat
	Subscriptions() SubscriptionsStat
	Clients() Clients
	Sessions() Sessions
}

// Metric is wrap around all of metrics
type Metric interface {
	Bytes() BytesMetric
	Packets() PacketsMetric
}

// PacketsMetric packets metric
type PacketsMetric interface {
	Sent(t mqttp.Type)
	Received(t mqttp.Type)
}

// BytesMetric bytes metric
type BytesMetric interface {
	Sent(bytes uint64)
	Received(bytes uint64)
}

// Sessions Statistic of sessions
type Sessions interface {
	Created(id string, status *SessionCreatedStatus)
	Removed(id string, status *SessionDeletedStatus)
}

// Clients Statistic of sessions
type Clients interface {
	Connected(id string, status *ClientConnectStatus)
	Disconnected(id string, reason mqttp.ReasonCode)
}

// TopicsStat statistic of topics
type TopicsStat interface {
	Added()
	Removed()
}

// SubscriptionsStat statistic of subscriptions
type SubscriptionsStat interface {
	Subscribed()
	UnSubscribed()
}
