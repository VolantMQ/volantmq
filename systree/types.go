package systree

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/troian/surgemq/packet"
)

// DynamicValue interface describes states of the dynamic value
type DynamicValue interface {
	Topic() string
	// Retained used by topics provider to get retained message when there is new subscription to given topic
	Retained() *packet.Publish
	// Publish used by systree update routine to publish new value when on periodic basis
	Publish() *packet.Publish
}

type dynamicValue struct {
	topic    string
	retained *packet.Publish
	publish  *packet.Publish
	getValue func() []byte
}

type dynamicValueInteger struct {
	dynamicValue
	val uint64
}

type dynamicValueUpTime struct {
	dynamicValue
	startTime time.Time
}

type dynamicValueCurrentTime struct {
	dynamicValue
}

func newDynamicValueInteger(topic string) *dynamicValueInteger {
	v := &dynamicValueInteger{}
	v.topic = topic
	v.getValue = v.get

	return v
}

func newDynamicValueUpTime(topic string) *dynamicValueUpTime {
	v := &dynamicValueUpTime{
		startTime: time.Now(),
	}

	v.topic = topic
	v.getValue = v.get

	return v
}

func newDynamicValueCurrentTime(topic string) *dynamicValueCurrentTime {
	v := &dynamicValueCurrentTime{}
	v.topic = topic
	v.getValue = v.get

	return v
}

func (v *dynamicValueInteger) get() []byte {
	val := strconv.FormatUint(atomic.LoadUint64(&v.val), 10)
	return []byte(val)
}

func (v *dynamicValueUpTime) get() []byte {
	diff := time.Since(v.startTime)

	return []byte(diff.String())
}

func (v *dynamicValueCurrentTime) get() []byte {
	val := time.Now().Format(time.RFC3339)
	return []byte(val)
}

func (m *dynamicValue) Topic() string {
	return m.topic
}

func (m *dynamicValue) Retained() *packet.Publish {
	if m.retained == nil {
		np, _ := packet.NewMessage(packet.ProtocolV311, packet.PUBLISH)
		m.retained, _ = np.(*packet.Publish)
		m.retained.SetTopic(m.topic)   // nolint: errcheck
		m.retained.SetQoS(packet.QoS0) // nolint: errcheck
		m.retained.SetRetain(true)
	}

	m.retained.SetPayload(m.getValue())

	return m.retained
}

func (m *dynamicValue) Publish() *packet.Publish {
	if m.publish == nil {
		np, _ := packet.NewMessage(packet.ProtocolV311, packet.PUBLISH)
		m.publish, _ = np.(*packet.Publish)
		m.publish.SetTopic(m.topic)   // nolint: errcheck
		m.publish.SetQoS(packet.QoS0) // nolint: errcheck
		m.publish.SetRetain(true)
	}

	m.publish.SetPayload(m.getValue())

	return m.publish
}
