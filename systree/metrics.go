package systree

import (
	"sync/atomic"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/volantmq/types"
)

type metricEntry struct {
	sent *dynamicValueInteger
	recv *dynamicValueInteger
}

type packetsMetric struct {
	total       *metricEntry
	connect     *metricEntry
	connAck     *metricEntry
	publish     *metricEntry
	subscribe   *metricEntry
	suback      *metricEntry
	unsubscribe *metricEntry
	unSubAck    *metricEntry
	pingReq     *metricEntry
	pingResp    *metricEntry
	disconnect  *metricEntry
	auth        *metricEntry
}

type bytesMetric struct {
	metricEntry
}

type metric struct {
	packets *packetsMetric
	bytes   *bytesMetric
}

func newMetricEntry(topicPrefix string, retained *[]types.RetainObject) *metricEntry {
	m := &metricEntry{
		sent: newDynamicValueInteger(topicPrefix + "/sent"),
		recv: newDynamicValueInteger(topicPrefix + "/received"),
	}

	*retained = append(*retained, m.sent, m.recv)
	return m
}

func newBytesMetric(topicPrefix string, retained *[]types.RetainObject) *bytesMetric {
	return &bytesMetric{
		metricEntry: *newMetricEntry(topicPrefix+"/bytes", retained),
	}
}

func newMetric(topicPrefix string, retained *[]types.RetainObject) metric {
	return metric{
		packets: newPacketsMetric(topicPrefix+"/metrics", retained),
		bytes:   newBytesMetric(topicPrefix+"/metrics", retained),
	}
}

func newPacketsMetric(topicPrefix string, retained *[]types.RetainObject) *packetsMetric {
	return &packetsMetric{
		total:       newMetricEntry(topicPrefix+"/packets/total", retained),
		connect:     newMetricEntry(topicPrefix+"/packets/connect", retained),
		connAck:     newMetricEntry(topicPrefix+"/packets/connack", retained),
		publish:     newMetricEntry(topicPrefix+"/packets/publish", retained),
		subscribe:   newMetricEntry(topicPrefix+"/packets/subscribe", retained),
		suback:      newMetricEntry(topicPrefix+"/packets/suback", retained),
		unsubscribe: newMetricEntry(topicPrefix+"/packets/unsubscribe", retained),
		unSubAck:    newMetricEntry(topicPrefix+"/packets/unsuback", retained),
		pingReq:     newMetricEntry(topicPrefix+"/packets/pingreq", retained),
		pingResp:    newMetricEntry(topicPrefix+"/packets/pingresp", retained),
		disconnect:  newMetricEntry(topicPrefix+"/packets/disconnect", retained),
		auth:        newMetricEntry(topicPrefix+"/packets/auth", retained),
	}
}

// Sent add sent packet to metrics
func (t *packetsMetric) Sent(mt mqttp.Type) {
	atomic.AddUint64(&t.total.sent.val, 1)
	switch mt {
	case mqttp.CONNECT:
		atomic.AddUint64(&t.connect.sent.val, 1)
	case mqttp.CONNACK:
		atomic.AddUint64(&t.connAck.sent.val, 1)
	case mqttp.PUBLISH:
		atomic.AddUint64(&t.publish.sent.val, 1)
	case mqttp.SUBSCRIBE:
		atomic.AddUint64(&t.subscribe.sent.val, 1)
	case mqttp.SUBACK:
		atomic.AddUint64(&t.suback.sent.val, 1)
	case mqttp.UNSUBSCRIBE:
		atomic.AddUint64(&t.unsubscribe.sent.val, 1)
	case mqttp.UNSUBACK:
		atomic.AddUint64(&t.unSubAck.sent.val, 1)
	case mqttp.PINGREQ:
		atomic.AddUint64(&t.pingReq.sent.val, 1)
	case mqttp.PINGRESP:
		atomic.AddUint64(&t.pingResp.sent.val, 1)
	case mqttp.DISCONNECT:
		atomic.AddUint64(&t.disconnect.sent.val, 1)
	case mqttp.AUTH:
		atomic.AddUint64(&t.auth.sent.val, 1)
	}
}

// Received add received packet to metrics
func (t *packetsMetric) Received(mt mqttp.Type) {
	atomic.AddUint64(&t.total.recv.val, 1)
	switch mt {
	case mqttp.CONNECT:
		atomic.AddUint64(&t.connect.recv.val, 1)
	case mqttp.CONNACK:
		atomic.AddUint64(&t.connAck.recv.val, 1)
	case mqttp.PUBLISH:
		atomic.AddUint64(&t.publish.recv.val, 1)
	case mqttp.SUBSCRIBE:
		atomic.AddUint64(&t.subscribe.recv.val, 1)
	case mqttp.SUBACK:
		atomic.AddUint64(&t.suback.recv.val, 1)
	case mqttp.UNSUBSCRIBE:
		atomic.AddUint64(&t.unsubscribe.recv.val, 1)
	case mqttp.UNSUBACK:
		atomic.AddUint64(&t.unSubAck.recv.val, 1)
	case mqttp.PINGREQ:
		atomic.AddUint64(&t.pingReq.recv.val, 1)
	case mqttp.PINGRESP:
		atomic.AddUint64(&t.pingResp.recv.val, 1)
	case mqttp.DISCONNECT:
		atomic.AddUint64(&t.disconnect.recv.val, 1)
	case mqttp.AUTH:
		atomic.AddUint64(&t.auth.recv.val, 1)
	}
}

// Bytes get bytes metric provider
func (t *metric) Bytes() BytesMetric {
	return t.bytes
}

// Packets get packets metric provider
func (t *metric) Packets() PacketsMetric {
	return t.packets
}

// Sent add sent bytes to statistic
func (t *bytesMetric) Sent(bytes uint64) {
	atomic.AddUint64(&t.sent.val, bytes)
}

// Received add received bytes to statistic
func (t *bytesMetric) Received(bytes uint64) {
	atomic.AddUint64(&t.recv.val, bytes)
}
