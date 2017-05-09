// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"encoding/binary"
	"fmt"
	"strings"
)

const (
	maxLPString uint16 = 65535
	//maxFixedHeaderLength int    = 5
	maxRemainingLength int32 = 268435455 // bytes, or 256 MB
)

// QosType QoS type
type QosType byte

const (
	// QosAtMostOnce QoS 0: At most once delivery
	// The message is delivered according to the capabilities of the underlying network.
	// No response is sent by the receiver and no retry is performed by the sender. The
	// message arrives at the receiver either once or not at all.
	QosAtMostOnce QosType = iota

	// QosAtLeastOnce QoS 1: At least once delivery
	// This quality of service ensures that the message arrives at the receiver at least once.
	// A QoS 1 PUBLISH Packet has a Packet Identifier in its variable header and is acknowledged
	// by a PUBACK Packet. Section 2.3.1 provides more information about Packet Identifiers.
	QosAtLeastOnce

	// QosExactlyOnce QoS 2: Exactly once delivery
	// This is the highest quality of service, for use when neither loss nor duplication of
	// messages are acceptable. There is an increased overhead associated with this quality of
	// service.
	QosExactlyOnce

	// QosFailure is a return value for a subscription if there's a problem while subscribing
	// to a specific topic.
	QosFailure = 0x80
)

// IsValid checks the QoS value to see if it's valid. Valid QoS are QosAtMostOnce,
// QosAtLeastOnce, and QosExactlyOnce.
func (c QosType) IsValid() bool {
	return c == QosAtMostOnce || c == QosAtLeastOnce || c == QosExactlyOnce
}

// IsValidFull checks the QoS value to see if it's valid. Valid QoS are QosAtMostOnce,
// QosAtLeastOnce, QosExactlyOnce and QosFailure.
func (c QosType) IsValidFull() bool {
	if c != QosAtMostOnce && c != QosAtLeastOnce && c != QosExactlyOnce && c != QosFailure {
		return false
	}

	return true
}

// SupportedVersions is a map of the version number (0x3 or 0x4) to the version string,
// "MQIsdp" for 0x3, and "MQTT" for 0x4.
var SupportedVersions = map[byte]string{
	0x3: "MQIsdp",
	0x4: "MQTT",
}

// Type is the type representing the MQTT packet types. In the MQTT spec,
// MQTT control packet type is represented as a 4-bit unsigned value.
type Type byte

// Topics slice of topics
type Topics []string

// TopicsQoS is a map if topic and corresponding QoS
type TopicsQoS map[string]QosType

// Provider is an interface defined for all MQTT message types.
type Provider interface {
	// Name returns a string representation of the message type. Examples include
	// "PUBLISH", "SUBSCRIBE", and others. This is statically defined for each of
	// the message types and cannot be changed.
	Name() string

	// Desc returns a string description of the message type. For example, a
	// CONNECT message would return "Client request to connect to Server." These
	// descriptions are statically defined (copied from the MQTT spec) and cannot
	// be changed.
	Desc() string

	// Type returns the MessageType of the Message. The retured value should be one
	// of the constants defined for MessageType.
	Type() Type

	// PacketID returns the packet ID of the Message. The retured value is 0 if
	// there's no packet ID for this message type. Otherwise non-0.
	PacketID() uint16

	// Encode writes the message bytes into the byte array from the argument. It
	// returns the number of bytes encoded and whether there's any errors along
	// the way. If there's any errors, then the byte slice and count should be
	// considered invalid.
	Encode([]byte) (int, error)

	// Decode reads the bytes in the byte slice from the argument. It returns the
	// total number of bytes decoded, and whether there's any errors during the
	// process. The byte slice MUST NOT be modified during the duration of this
	// message being available since the byte slice is internally stored for
	// references.
	Decode([]byte) (int, error)

	Len() int
}

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

// Error errors
type Error byte

const (
	// ErrInvalidUnSubscribe Invalid UNSUBSCRIBE message
	ErrInvalidUnSubscribe Error = iota
	// ErrInvalidUnSubAck Invalid UNSUBACK message
	ErrInvalidUnSubAck
	// ErrPackedIDNotMatched Packet ID does not match
	ErrPackedIDNotMatched
	// ErrOnPublishNil Publisher is nil
	ErrOnPublishNil
	// ErrInvalidMessageType Invalid message type
	ErrInvalidMessageType
	// ErrInvalidMessageTypeFlags Invalid message flags
	ErrInvalidMessageTypeFlags
	// ErrInvalidQoS Invalid message QoS
	ErrInvalidQoS
	// ErrInvalidLength Invalid message length
	ErrInvalidLength
	// ErrProtocolViolation Message Will violation
	ErrProtocolViolation
	// ErrInsufficientBufferSize Insufficient buffer size
	ErrInsufficientBufferSize
	// ErrInvalidTopic Topic is empty
	ErrInvalidTopic
	// ErrEmptyPayload Payload is empty
	ErrEmptyPayload
	// ErrInvalidReturnCode invalid return code
	ErrInvalidReturnCode
)

// Error returns the corresponding error string for the ConnAckCode
func (e Error) Error() string {
	switch e {
	case ErrInvalidUnSubscribe:
		return "Invalid UNSUBSCRIBE message"
	case ErrInvalidUnSubAck:
		return "Invalid UNSUBACK message"
	case ErrPackedIDNotMatched:
		return "Packet ID does not match"
	case ErrOnPublishNil:
		return "Publisher is nil"
	case ErrInvalidMessageType:
		return "Invalid message type"
	case ErrInvalidMessageTypeFlags:
		return "Invalid message flags"
	case ErrInvalidQoS:
		return "Invalid message QoS"
	case ErrInvalidLength:
		return "Invalid message length"
	case ErrProtocolViolation:
		return "Protocol violation"
	case ErrInsufficientBufferSize:
		return "Insufficient buffer size"
	case ErrInvalidTopic:
		return "Invalid topic name"
	case ErrEmptyPayload:
		return "Payload is empty"
	case ErrInvalidReturnCode:
		return "Invalid return code"
	}

	return "Unknown error"
}

func (mt Type) String() string {
	return mt.Name()
}

// Name returns the name of the message type. It should correspond to one of the
// constant values defined for MessageType. It is statically defined and cannot
// be changed.
func (mt Type) Name() string {
	switch mt {
	case RESERVED:
		return "RESERVED"
	case CONNECT:
		return "CONNECT"
	case CONNACK:
		return "CONNACK"
	case PUBLISH:
		return "PUBLISH"
	case PUBACK:
		return "PUBACK"
	case PUBREC:
		return "PUBREC"
	case PUBREL:
		return "PUBREL"
	case PUBCOMP:
		return "PUBCOMP"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case SUBACK:
		return "SUBACK"
	case UNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case UNSUBACK:
		return "UNSUBACK"
	case PINGREQ:
		return "PINGREQ"
	case PINGRESP:
		return "PINGRESP"
	case DISCONNECT:
		return "DISCONNECT"
	case RESERVED2:
		return "RESERVED2"
	}

	return "UNKNOWN"
}

// Desc returns the description of the message type. It is statically defined (copied
// from MQTT spec) and cannot be changed.
func (mt Type) Desc() string {
	switch mt {
	case RESERVED:
		return "Reserved"
	case CONNECT:
		return "Client request to connect to Server"
	case CONNACK:
		return "Connect acknowledgement"
	case PUBLISH:
		return "Publish message"
	case PUBACK:
		return "Publish acknowledgement"
	case PUBREC:
		return "Publish received (assured delivery part 1)"
	case PUBREL:
		return "Publish release (assured delivery part 2)"
	case PUBCOMP:
		return "Publish complete (assured delivery part 3)"
	case SUBSCRIBE:
		return "Client subscribe request"
	case SUBACK:
		return "Subscribe acknowledgement"
	case UNSUBSCRIBE:
		return "Unsubscribe request"
	case UNSUBACK:
		return "Unsubscribe acknowledgement"
	case PINGREQ:
		return "PING request"
	case PINGRESP:
		return "PING response"
	case DISCONNECT:
		return "Client is disconnecting"
	case RESERVED2:
		return "Reserved"
	}

	return "UNKNOWN"
}

// DefaultFlags returns the default flag values for the message type, as defined by
// the MQTT spec.
func (mt Type) DefaultFlags() byte {
	switch mt {
	case RESERVED:
		return 0
	case CONNECT:
		return 0
	case CONNACK:
		return 0
	case PUBLISH:
		return 0
	case PUBACK:
		return 0
	case PUBREC:
		return 0
	case PUBREL:
		return 2
	case PUBCOMP:
		return 0
	case SUBSCRIBE:
		return 2
	case SUBACK:
		return 0
	case UNSUBSCRIBE:
		return 2
	case UNSUBACK:
		return 0
	case PINGREQ:
		return 0
	case PINGRESP:
		return 0
	case DISCONNECT:
		return 0
	case RESERVED2:
		return 0
	}

	return 0
}

// New creates a new message based on the message type. It is a shortcut to call
// one of the New*Message functions. If an error is returned then the message type
// is invalid.
func (mt Type) New() (Provider, error) {
	switch mt {
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
	}

	return nil, ErrInvalidMessageType
}

// Valid returns a boolean indicating whether the message type is valid or not.
func (mt Type) Valid() bool {
	return mt > RESERVED && mt < RESERVED2
}

// ValidTopic checks the topic, which is a slice of bytes, to see if it's valid. Topic is
// considered valid if it's longer than 0 bytes, and doesn't contain any wildcard characters
// such as + and #.
func ValidTopic(topic string) bool {
	return len(topic) > 0 && !strings.Contains(topic, "#") && !strings.Contains(topic, "+")
}

// ValidVersion checks to see if the version is valid. Current supported versions include 0x3 and 0x4.
func ValidVersion(v byte) bool {
	_, ok := SupportedVersions[v]
	return ok
}

// ValidConnAckError checks to see if the error is a ConnAck Error or not
func ValidConnAckError(err error) (ConnAckCode, bool) {
	if code, ok := err.(ConnAckCode); ok {
		return code, true
	}

	return 0, false
	//return err == ErrInvalidProtocolVersion || err == ErrIdentifierRejected ||
	//	err == ErrServerUnavailable || err == ErrBadUsernameOrPassword || err == ErrNotAuthorized
}

// Read length prefixed bytes
func readLPBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 2 {
		return nil, 0, ErrInsufficientBufferSize
	}

	var n int
	total := 0

	n = int(binary.BigEndian.Uint16(buf))
	total += 2

	if len(buf) < n {
		return nil, total, ErrInsufficientBufferSize
	}

	total += n

	return buf[2:total], total, nil
}

// Write length prefixed bytes
func writeLPBytes(buf []byte, b []byte) (int, error) {
	total, n := 0, len(b)

	if n > int(maxLPString) {
		return 0, fmt.Errorf("utils/writeLPBytes: Length (%d) greater than %d bytes", n, maxLPString)
	}

	if len(buf) < 2+n {
		return 0, ErrInsufficientBufferSize
	}

	binary.BigEndian.PutUint16(buf, uint16(n))
	total += 2

	copy(buf[total:], b)
	total += n

	return total, nil
}
