package message

// ProtocolVersion
type ProtocolVersion byte

const (
	ProtocolV31  = ProtocolVersion(0x3)
	ProtocolV311 = ProtocolVersion(0x4)
	ProtocolV50  = ProtocolVersion(0x5)
)

// SupportedVersions is a map of the version number (0x3 or 0x4) to the version string,
// "MQIsdp" for 0x3, and "MQTT" for 0x4.
var SupportedVersions = map[ProtocolVersion]string{
	ProtocolV31:  "MQIsdp",
	ProtocolV311: "MQTT",
	ProtocolV50:  "MQTT",
}

const (
	// MaxLPString maximum size of length-prefixed string
	MaxLPString = 65535
)
