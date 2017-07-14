package message

// Type is the type representing the MQTT packet types. In the MQTT spec,
// MQTT control packet type is represented as a 4-bit unsigned value.
type Type byte

const (
	// RESERVED is a reserved value and should be considered an invalid message type
	RESERVED Type = iota

	// CONNECT Client to Server. Client request to connect to Server.
	CONNECT

	// CONNACK Server to Client. Connect acknowledgement.
	CONNACK

	// PUBLISH Client to Server, or Server to Client. Publish message.
	PUBLISH

	// PUBACK Client to Server, or Server to Client. Publish acknowledgment for
	// QoS 1 messages.
	PUBACK

	// PUBREC Client to Server, or Server to Client. Publish received for QoS 2 messages.
	// Assured delivery part 1.
	PUBREC

	// PUBREL Client to Server, or Server to Client. Publish release for QoS 2 messages.
	// Assured delivery part 1.
	PUBREL

	// PUBCOMP Client to Server, or Server to Client. Publish complete for QoS 2 messages.
	// Assured delivery part 3.
	PUBCOMP

	// SUBSCRIBE Client to Server. Client subscribe request.
	SUBSCRIBE

	// SUBACK Server to Client. Subscribe acknowledgement.
	SUBACK

	// UNSUBSCRIBE Client to Server. Unsubscribe request.
	UNSUBSCRIBE

	// UNSUBACK Server to Client. Unsubscribe acknowlegment.
	UNSUBACK

	// PINGREQ Client to Server. PING request.
	PINGREQ

	// PINGRESP Server to Client. PING response.
	PINGRESP

	// DISCONNECT Client to Server. Client is disconnecting.
	DISCONNECT

	// RESERVED2 is a reserved value and should be considered an invalid message type.
	RESERVED2
)

var typeName = [RESERVED2 + 1]string{
	"RESERVED",
	"CONNECT",
	"CONNACK",
	"PUBLISH",
	"PUBACK",
	"PUBREC",
	"PUBREL",
	"PUBCOMP",
	"SUBSCRIBE",
	"SUBACK",
	"UNSUBSCRIBE",
	"UNSUBACK",
	"PINGREQ",
	"PINGRESP",
	"DISCONNECT",
	"RESERVED2",
}

var typeDescription = [RESERVED2 + 1]string{
	"Reserved",
	"Client request to connect to Server",
	"Connect acknowledgement",
	"Publish message",
	"Publish acknowledgement",
	"Publish received (assured delivery part 1)",
	"Publish release (assured delivery part 2)",
	"Publish complete (assured delivery part 3)",
	"Client subscribe request",
	"Subscribe acknowledgement",
	"Unsubscribe request",
	"Unsubscribe acknowledgement",
	"PING request",
	"PING response",
	"Client is disconnecting",
	"Reserved",
}

var typeDefaultFlags = [RESERVED2 + 1]byte{
	0, // RESERVED
	0, // CONNECT
	0, // CONNACK
	0, // PUBLISH
	0, // PUBACK
	0, // PUBREC
	2, // PUBREL
	0, // PUBCOMP
	2, // SUBSCRIBE
	0, // SUBACK
	2, // UNSUBSCRIBE
	0, // UNSUBACK
	0, // PINGREQ
	0, // PINGRESP
	0, // DISCONNECT
	0, // RESERVED2
}

// Name returns the name of the message type. It should correspond to one of the
// constant values defined for MessageType. It is statically defined and cannot
// be changed.
func (t Type) Name() string {
	if t > RESERVED2 {
		return "UNKNOWN"
	}

	return typeName[t]
}

// Desc returns the description of the message type. It is statically defined (copied
// from MQTT spec) and cannot be changed.
func (t Type) Desc() string {
	if t > RESERVED2 {
		return "UNKNOWN"
	}

	return typeDescription[t]
}

// DefaultFlags returns the default flag values for the message type, as defined by the MQTT spec.
func (t Type) DefaultFlags() byte {
	if t > RESERVED2 {
		return 0
	}

	return typeDefaultFlags[t]
}

// NewMessage creates a new message based on the message type. It is a shortcut to call
// one of the New*Message functions. If an error is returned then the message type
// is invalid.
func (t Type) NewMessage() (Provider, error) {
	switch t {
	case CONNECT:
		return NewConnectMessage(), nil
	case CONNACK:
		return NewConnAckMessage(), nil
	case PUBLISH:
		return NewPublishMessage(), nil
	case PUBACK:
		return NewPubAckMessage(), nil
	case PUBREC:
		return NewPubRecMessage(), nil
	case PUBREL:
		return NewPubRelMessage(), nil
	case PUBCOMP:
		return NewPubCompMessage(), nil
	case SUBSCRIBE:
		return NewSubscribeMessage(), nil
	case SUBACK:
		return NewSubAckMessage(), nil
	case UNSUBSCRIBE:
		return NewUnSubscribeMessage(), nil
	case UNSUBACK:
		return NewUnSubAckMessage(), nil
	case PINGREQ:
		return NewPingReqMessage(), nil
	case PINGRESP:
		return NewPingRespMessage(), nil
	case DISCONNECT:
		return NewDisconnectMessage(), nil
	default:
		return nil, ErrInvalidMessageType
	}
}

// NewMessage creates a new message based on the message type. It is a shortcut to call
// one of the New*Message functions. If an error is returned then the message type
// is invalid.
func NewMessage(t Type) (Provider, error) {
	return t.NewMessage()
}

// Valid returns a boolean indicating whether the message type is valid or not.
func (t Type) Valid() bool {
	return t > RESERVED && t < RESERVED2
}
