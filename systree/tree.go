package systree

import (
	"github.com/troian/surgemq/message"
	"math"
	"sync/atomic"
)

// Provider systree provider
type Provider interface {
	Metric() Metric
	Topics() TopicsStat
	Session() SessionStat
	Sessions() SessionsStat
}

// Metric is wrap around all of metrics
type Metric interface {
	Bytes() BytesMetric
	Packets() PacketsMetric
}

// PacketsMetric packets metric
type PacketsMetric interface {
	Sent(t message.Type)
	Received(t message.Type)
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

// TopicsStat statistic of topics
type TopicsStat interface {
	Added()
	Removed()
}

// SessionStat statistic of session
type SessionStat interface {
	Connected()
	Disconnected()
	Subscribed()
	UnSubscribed()
}

type sessionsStat struct {
	curr uint64
	max  uint64
}

type topicsStat struct {
	curr uint64
	max  uint64
}

type sessionStat struct {
	clients struct {
		curr uint64
		max  uint64
	}

	subs struct {
		curr uint64
		max  uint64
	}
}

type metric struct {
	packets packetsMetric
	bytes   bytesMetric
}

type packetsMetric struct {
	total struct {
		sent     uint64
		received uint64
	}

	connect struct {
		sent     uint64
		received uint64
	}
	connAck struct {
		sent     uint64
		received uint64
	}
	publish struct {
		sent     uint64
		received uint64
	}

	subscribe struct {
		sent     uint64
		received uint64
	}
	suback struct {
		sent     uint64
		received uint64
	}
	unsubscribe struct {
		sent     uint64
		received uint64
	}
	unSubAck struct {
		sent     uint64
		received uint64
	}
	pingReq struct {
		sent     uint64
		received uint64
	}
	pingResp struct {
		sent     uint64
		received uint64
	}
	disconnect struct {
		sent     uint64
		received uint64
	}
}

type bytesMetric struct {
	sent     uint64
	received uint64
}

type impl struct {
	metrics  metric
	topics   topicsStat
	session  sessionStat
	sessions sessionsStat
}

// NewTree allocate systree provider
func NewTree() (Provider, error) {
	tr := &impl{}

	return tr, nil
}

// Sessions get sessions stat provider
func (t *impl) Sessions() SessionsStat {
	return &t.sessions
}

// Session get session stat provider
func (t *impl) Session() SessionStat {
	return &t.session
}

// Topics get topics stat provider
func (t *impl) Topics() TopicsStat {
	return &t.topics
}

// Metric get metric provider
func (t *impl) Metric() Metric {
	return &t.metrics
}

// Bytes get bytes metric provider
func (t *metric) Bytes() BytesMetric {
	return &t.bytes
}

// Packets get packets metric provider
func (t *metric) Packets() PacketsMetric {
	return &t.packets
}

// Sent add sent bytes to statistic
func (t *bytesMetric) Sent(bytes uint64) {
	atomic.AddUint64(&t.sent, bytes)
}

// Received add received bytes to statistic
func (t *bytesMetric) Received(bytes uint64) {
	atomic.AddUint64(&t.received, bytes)
}

// Created add to statistic new session
func (t *sessionsStat) Created() {
	newVal := atomic.AddUint64(&t.curr, 1)
	if atomic.LoadUint64(&t.max) < newVal {
		atomic.StoreUint64(&t.max, newVal)
	}
}

// Removed remove from statistic session
func (t *sessionsStat) Removed() {
	atomic.AddUint64(&t.curr, ^uint64(math.MaxUint64-1))
}

// Connected add to statistic new client
func (t *sessionStat) Connected() {
	newVal := atomic.AddUint64(&t.clients.curr, 1)
	if atomic.LoadUint64(&t.clients.max) < newVal {
		atomic.StoreUint64(&t.clients.max, newVal)
	}
}

// Disconnected remove client from statistic
func (t *sessionStat) Disconnected() {
	atomic.AddUint64(&t.clients.curr, ^uint64(math.MaxUint64-1))
}

// Subscribed add to statistic subscriber
func (t *sessionStat) Subscribed() {
	newVal := atomic.AddUint64(&t.subs.curr, 1)
	if atomic.LoadUint64(&t.subs.max) < newVal {
		atomic.StoreUint64(&t.subs.max, newVal)
	}
}

// UnSubscribed remove subscriber from statistic
func (t *sessionStat) UnSubscribed() {
	atomic.AddUint64(&t.subs.curr, ^uint64(math.MaxUint64-1))
}

// Added add topic to statistic
func (t *topicsStat) Added() {
	newVal := atomic.AddUint64(&t.curr, 1)
	if atomic.LoadUint64(&t.max) < newVal {
		atomic.StoreUint64(&t.max, newVal)
	}
}

// Removed remove topic from statistic
func (t *topicsStat) Removed() {
	atomic.AddUint64(&t.curr, ^uint64(math.MaxUint64-1))
}

// Sent add sent packet to metrics
func (t *packetsMetric) Sent(mt message.Type) {
	atomic.AddUint64(&t.total.sent, 1)
	switch mt {
	case message.CONNECT:
		atomic.AddUint64(&t.connect.sent, 1)
	case message.CONNACK:
		atomic.AddUint64(&t.connAck.sent, 1)
	case message.PUBLISH:
		atomic.AddUint64(&t.publish.sent, 1)
	case message.SUBSCRIBE:
		atomic.AddUint64(&t.subscribe.sent, 1)
	case message.SUBACK:
		atomic.AddUint64(&t.suback.sent, 1)
	case message.UNSUBSCRIBE:
		atomic.AddUint64(&t.unsubscribe.sent, 1)
	case message.UNSUBACK:
		atomic.AddUint64(&t.unSubAck.sent, 1)
	case message.PINGREQ:
		atomic.AddUint64(&t.pingReq.sent, 1)
	case message.PINGRESP:
		atomic.AddUint64(&t.pingResp.sent, 1)
	case message.DISCONNECT:
		atomic.AddUint64(&t.disconnect.sent, 1)
	}
}

// Received add received packet to metrics
func (t *packetsMetric) Received(mt message.Type) {
	atomic.AddUint64(&t.total.received, 1)
	switch mt {
	case message.CONNECT:
		atomic.AddUint64(&t.connect.received, 1)
	case message.CONNACK:
		atomic.AddUint64(&t.connAck.received, 1)
	case message.PUBLISH:
		atomic.AddUint64(&t.publish.received, 1)
	case message.SUBSCRIBE:
		atomic.AddUint64(&t.subscribe.received, 1)
	case message.SUBACK:
		atomic.AddUint64(&t.suback.received, 1)
	case message.UNSUBSCRIBE:
		atomic.AddUint64(&t.unsubscribe.received, 1)
	case message.UNSUBACK:
		atomic.AddUint64(&t.unSubAck.received, 1)
	case message.PINGREQ:
		atomic.AddUint64(&t.pingReq.received, 1)
	case message.PINGRESP:
		atomic.AddUint64(&t.pingResp.received, 1)
	case message.DISCONNECT:
		atomic.AddUint64(&t.disconnect.received, 1)
	}
}
