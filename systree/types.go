package systree

import (
	"strconv"
	"sync/atomic"

	"time"

	"fmt"

	"github.com/troian/surgemq/message"
)

// DynamicValue interface describes states of the dynamic value
type DynamicValue interface {
	Topic() string
	// Retained used by topics provider to get retained message when there is new subscription to given topic
	Retained() *message.PublishMessage
	// Publish used by systree update routine to publish new value when on periodic basis
	Publish() *message.PublishMessage
}

type dynamicValue struct {
	topic    string
	retained *message.PublishMessage
	publish  *message.PublishMessage
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

	val := fmt.Sprintf("%s", diff)

	return []byte(val)
}

func (v *dynamicValueCurrentTime) get() []byte {
	val := time.Now().Format(time.RFC3339)
	return []byte(val)
}

func (m *dynamicValue) Topic() string {
	return m.topic
}

func (m *dynamicValue) Retained() *message.PublishMessage {
	if m.retained == nil {
		np, _ := message.NewMessage(message.ProtocolV311, message.PUBLISH)
		m.retained, _ = np.(*message.PublishMessage)
		m.retained.SetTopic(m.topic)
		m.retained.SetQoS(message.QoS0)
		m.retained.SetRetain(true)
	}

	m.retained.SetPayload(m.getValue())

	return m.retained
}

func (m *dynamicValue) Publish() *message.PublishMessage {
	if m.publish == nil {
		np, _ := message.NewMessage(message.ProtocolV311, message.PUBLISH)
		m.publish, _ = np.(*message.PublishMessage)
		m.publish.SetTopic(m.topic)
		m.publish.SetQoS(message.QoS0)
		m.publish.SetRetain(true)
	}

	m.publish.SetPayload(m.getValue())

	return m.publish
}
