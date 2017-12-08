package packet

// Type is the type representing the MQTT packet types. In the MQTT spec,
// MQTT control packet type is represented as a 4-bit unsigned value.
type Type byte

// IDType as per [MQTT-2.2.1]
type IDType uint16

const (
	// RESERVED is a reserved value and should be considered an invalid message type
	RESERVED Type = iota

	// CONNECT Client request to connect to Server
	// version: v3.1, v3.1.1, v5.0
	//      Dir: Client to Server
	CONNECT

	// CONNACK Accept acknowledgement
	// version: v3.1, v3.1.1, v5.0
	//      Dir: Server to Client
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

	// AUTH is a reserved value and should be considered an invalid message type.
	AUTH
)

var typeName = [AUTH + 1]string{
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
	"AUTH",
}

var typeDescription = [AUTH + 1]string{
	"Reserved",
	"Client request to connect to Server",
	"Accept acknowledgement",
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
	"Auth",
}

var typeDefaultFlags = [AUTH + 1]byte{
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
	0, // AUTH
}

// Name returns the name of the message type. It should correspond to one of the
// constant values defined for MessageType. It is statically defined and cannot
// be changed.
func (t Type) Name() string {
	if t > AUTH {
		return "UNKNOWN"
	}

	return typeName[t]
}

// Desc returns the description of the message type. It is statically defined (copied
// from MQTT spec) and cannot be changed.
func (t Type) Desc() string {
	if t > AUTH {
		return "UNKNOWN"
	}

	return typeDescription[t]
}

// DefaultFlags returns the default flag values for the message type, as defined by the MQTT spec.
func (t Type) DefaultFlags() byte {
	if t > AUTH {
		return 0
	}

	return typeDefaultFlags[t]
}

// IsValid check if protocol version is valid for this implementation
func (p ProtocolVersion) IsValid() bool {
	_, ok := SupportedVersions[p]
	return ok
}

// Valid returns a boolean indicating whether the message type is valid or not.
func (t Type) Valid(v ProtocolVersion) (bool, error) {
	switch v {
	case ProtocolV31:
		fallthrough
	case ProtocolV311:
		return t > RESERVED && t < AUTH, nil
	case ProtocolV50:
		return t > RESERVED && t <= AUTH, nil
	default:
		return false, ErrInvalidProtocolVersion
	}
}
