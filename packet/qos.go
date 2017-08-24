package packet

// QosType QoS type
type QosType byte

const (
	// QoS0 At most once delivery
	// The message is delivered according to the capabilities of the underlying network.
	// No response is sent by the receiver and no retry is performed by the sender. The
	// message arrives at the receiver either once or not at all.
	QoS0 QosType = iota

	// QoS1 At least once delivery
	// This quality of service ensures that the message arrives at the receiver at least once.
	// A QoS 1 PUBLISH Packet has a Packet Identifier in its variable header and is acknowledged
	// by a PUBACK Packet. Section 2.3.1 provides more information about Packet Identifiers.
	QoS1

	// QoS2 Exactly once delivery
	// This is the highest quality of service, for use when neither loss nor duplication of
	// messages are acceptable. There is an increased overhead associated with this quality of
	// service.
	QoS2

	// QosFailure is a return value for a subscription if there's a problem while subscribing
	// to a specific topic.
	QosFailure = 0x80
)

// IsValid checks the QoS value to see if it's valid. Valid QoS are QoS0,
// QoS1, and QoS2.
func (c QosType) IsValid() bool {
	return c == QoS0 || c == QoS1 || c == QoS2
}

// IsValidFull checks the QoS value to see if it's valid. Valid QoS are QoS0,
// QoS1, QoS2 and QosFailure.
func (c QosType) IsValidFull() bool {
	if c != QoS0 && c != QoS1 && c != QoS2 && c != QosFailure {
		return false
	}

	return true
}

// Desc get string representation of QoS value
func (c QosType) Desc() string {
	switch c {
	case QoS0:
		return "QoS0"
	case QoS1:
		return "QoS1"
	case QoS2:
		return "QoS2"
	default:
		return "Invalid value"
	}
}
