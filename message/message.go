package message

import (
	"strings"
	"unicode/utf8"
)

const (
	maxLPString uint16 = 65535
	//maxFixedHeaderLength int    = 5
	maxRemainingLength int32 = (256 * 1024 * 1024) - 1 // 256 MB
)
const (
	//  maskHeaderType  byte = 0xF0
	//  maskHeaderFlags byte = 0x0F
	//  maskHeaderFlagQoS
	maskConnAckSessionPresent byte = 0x01
)

const (
	offsetHeaderType byte = 0x04

//  offsetHeaderFlags byte = 0
)

// SubscriptionOptions as per [MQTT-3.8.3.1]
type SubscriptionOptions byte

// TopicQos map containing topics as a keys with respective subscription options as value
type TopicQos map[string]SubscriptionOptions

// QoS quality of service
func (s SubscriptionOptions) QoS() QosType {
	return QosType(byte(s) & maskSubscriptionQoS)
}

// NL No Local option
//   if true Application Messages MUST NOT be forwarded to a connection with a ClientID equal
//   to the ClientID of the publishing connection
// V5.0 ONLY
func (s SubscriptionOptions) NL() bool {
	return (byte(s) & maskSubscriptionNL >> offsetSubscriptionNL) != 0
}

// RAP Retain As Published option
//   true: Application Messages forwarded using this subscription keep the RETAIN flag they were published with
//   false : Application Messages forwarded using this subscription have the RETAIN flag set to 0.
// Retained messages sent when the subscription is established have the RETAIN flag set to 1.
// V5.0 ONLY
func (s SubscriptionOptions) RAP() bool {
	return (byte(s) & maskSubscriptionRAP >> offsetSubscriptionRAP) != 0
}

// RetainHandling specifies whether retained messages are sent when the subscription is established.
// This does not affect the sending of retained messages at any point after the subscribe.
// If there are no retained messages matching the Topic Filter, all of these values act the same.
// The values are:
//    0 = Send retained messages at the time of the subscribe
//    1 = Send retained messages at subscribe only if the subscription does not currently exist
//    2 = Do not send retained messages at the time of the subscribe
// V5.0 ONLY
func (s SubscriptionOptions) RetainHandling() byte {
	return (byte(s) & maskSubscriptionRetainHandling) >> offsetSubscriptionRetainHandling
}

// Provider is an interface defined for all MQTT message types.
type Provider interface {
	// Desc returns a string description of the message type. For example, a
	// CONNECT message would return "Client request to connect to Server." These
	// descriptions are statically defined (copied from the MQTT spec) and cannot
	// be changed.
	Desc() string

	// Type returns the MessageType of the Message. The returned value should be one
	// of the constants defined for MessageType.
	Type() PacketType

	// PacketID returns packet id
	// if has not been set return ErrNotSet
	PacketID() (PacketID, error)

	// Encode writes the message bytes into the byte array from the argument. It
	// returns the number of bytes encoded and whether there's any errors along
	// the way. If there's any errors, then the byte slice and count should be
	// considered invalid.
	Encode([]byte) (int, error)

	// Size of whole message
	Size() (int, error)

	// SetVersion set protocol version used by message
	SetVersion(v ProtocolVersion)

	// Version get protocol version used by message
	Version() ProtocolVersion

	PropertyGet(PropertyID) (interface{}, error)

	PropertySet(PropertyID, interface{}) error

	PropertyForEach(func(PropertyID, interface{})) error

	// decode reads the bytes in the byte slice from the argument. It returns the
	// total number of bytes decoded, and whether there's any errors during the
	// process. The byte slice MUST NOT be modified during the duration of this
	// message being available since the byte slice is internally stored for
	// references.

	// decode implemented by header and performs decode of the fixed header with remaining length
	decode([]byte) (int, error)

	// encodeMessage must be implemented by each packet implementation and used by Encode to perform encode of the
	// variable header, payload and properties if any
	encodeMessage([]byte) (int, error)

	// decodeMessage must be implemented by each packet implementation and used by Decode to perform decode of the
	// variable header, payload and properties if any
	decodeMessage([]byte) (int, error)

	// must be implemented by each packet implementation and returns remaining length
	size() int

	// getHeader
	getHeader() *header

	// setType
	setType(t PacketType)
}

// NewMessage creates a new message based on the message type. It is a shortcut to call
// one of the New*Message functions. If an error is returned then the message type
// is invalid.
func NewMessage(v ProtocolVersion, t PacketType) (Provider, error) {
	var m Provider

	switch t {
	case CONNECT:
		m = newConnectMessage()
	case CONNACK:
		m = newConnAckMessage()
	case PUBLISH:
		m = newPublishMessage()
	case PUBACK:
		m = newPubAckMessage()
	case PUBREC:
		m = newPubRecMessage()
	case PUBREL:
		m = newPubRelMessage()
	case PUBCOMP:
		m = newPubCompMessage()
	case SUBSCRIBE:
		m = newSubscribeMessage()
	case SUBACK:
		m = newSubAckMessage()
	case UNSUBSCRIBE:
		m = newUnSubscribeMessage()
	case UNSUBACK:
		m = newUnSubAckMessage()
	case PINGREQ:
		m = newPingReqMessage()
	case PINGRESP:
		m = newPingRespMessage()
	case DISCONNECT:
		m = newDisconnectMessage()
	case AUTH:
		if v != ProtocolV50 {
			return nil, ErrInvalidMessageType
		}
		m = newAuthMessage()
	default:
		return nil, ErrInvalidMessageType
	}

	m.setType(t)

	h := m.getHeader()

	h.version = v
	h.cb.encode = m.encodeMessage
	h.cb.decode = m.decodeMessage
	h.cb.size = m.size

	return m, nil
}

// Decode buf into message and return Provider type
func Decode(v ProtocolVersion, buf []byte) (msg Provider, total int, err error) {
	defer func() {
		// TODO: this case might be improved
		// Panic might be provided during message decode with malformed len
		// For example on length-prefixed payloads/topics or properties:

		//   length prefix of payload with size 4 but actual payload size is 2
		//   |   payload
		//   |   |
		// 00040102
		// in that case buf[lpEndOffset:lpEndOffset+lpLen] will panic due to out-of-bound
		//
		// Ideally such cases should be handled by each message implementation
		// but it might be worth doing such checks (there might be many for each message) on each decode
		// as it is abnormal and server must close connection
		if r := recover(); r != nil {
			msg = nil
			total = 0
			err = ErrPanicDetected
		}
	}()

	if len(buf) < 1 {
		return nil, 0, ErrInsufficientBufferSize
	}

	// [MQTT-2.2]
	mType := PacketType(buf[0] >> offsetMessageType)

	// [MQTT-2.2.1] Type.NewMessage validates message type
	if msg, err = NewMessage(v, mType); err != nil {
		return nil, 0, err
	}

	if total, err = msg.decode(buf); err != nil {
		return nil, total, err
	}

	return msg, total, nil
}

// ValidTopic checks the topic, which is a slice of bytes, to see if it's valid. Topic is
// considered valid if it's longer than 0 bytes, and doesn't contain any wildcard characters
// such as + and #.
func ValidTopic(topic string) bool {
	return len(topic) > 0 &&
		utf8.Valid([]byte(topic)) &&
		!strings.Contains(topic, "#") &&
		!strings.Contains(topic, "+")
}
