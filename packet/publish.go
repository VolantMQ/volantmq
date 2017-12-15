// Copyright (c) 2014 The VolantMQ Authors. All rights reserved.
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

package packet

import (
	"time"
)

// Publish A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client
// to transport an Application Message.
type Publish struct {
	header

	payload   []byte
	topic     string
	publishID uintptr
	expireAt  time.Time
}

var _ Provider = (*Publish)(nil)

func newPublish() *Publish {
	return &Publish{}
}

// NewPublish creates a new PUBLISH packet
func NewPublish(v ProtocolVersion) *Publish {
	p := newPublish()
	p.init(PUBLISH, v, p.size, p.encodeMessage, p.decodeMessage)
	return p
}

// SetExpireAt time object
func (msg *Publish) SetExpireAt(tm time.Time) {
	msg.expireAt = tm
}

// Expired check if packet has elapsed it's time or not
// returns false if does not expire
func (msg *Publish) Expired() (time.Time, uint32, bool) {
	if !msg.expireAt.IsZero() {
		var df uint32
		expired := true
		now := time.Now()

		if msg.expireAt.After(now) {
			if df = uint32(msg.expireAt.Sub(now) / time.Second); df > 0 {
				expired = false
			}
		}

		return msg.expireAt, df, expired
	}

	return time.Time{}, 0, false
}

// Clone packet
// qos, topic, payload, retain and properties
func (msg *Publish) Clone(v ProtocolVersion) (*Publish, error) {
	// message version should be same as session as encode/decode depends on it
	pkt := NewPublish(msg.version)

	// [MQTT-3.3.1-9]
	// [MQTT-3.3.1-3]
	pkt.Set(msg.topic, msg.Payload(), msg.QoS(), msg.Retain(), false) // nolint: errcheck

	// clone expiration setting with no matter of version as expired publish packet
	// should not be delivered to V3 brokers when it expired
	pkt.expireAt = msg.expireAt

	if msg.version == ProtocolV50 && v == ProtocolV50 {
		// [MQTT-3.3.2-4] forward Payload Format
		if prop, ok := msg.properties.properties[PropertyPayloadFormat]; ok {
			if err := pkt.properties.Set(msg.mType, PropertyPayloadFormat, prop); err != nil {
				return nil, err
			}
		}

		// [MQTT-1892 3.3.2-15] forward Response Topic
		if prop, ok := msg.properties.properties[PropertyResponseTopic]; ok {
			if err := pkt.properties.Set(msg.mType, PropertyResponseTopic, prop); err != nil {
				return nil, err
			}
		}

		// [MQTT-1908 3.3.2-16] forward Correlation Data
		if prop, ok := msg.properties.properties[PropertyCorrelationData]; ok {
			if err := pkt.properties.Set(msg.mType, PropertyCorrelationData, prop); err != nil {
				return nil, err
			}
		}

		// [MQTT-3.3.2-17] forward User Property
		if prop, ok := msg.properties.properties[PropertyUserProperty]; ok {
			if err := pkt.properties.Set(msg.mType, PropertyUserProperty, prop); err != nil {
				return nil, err
			}
		}

		// [MQTT-3.3.2-20] forward Content Type
		if prop, ok := msg.properties.properties[PropertyContentType]; ok {
			if err := pkt.properties.Set(msg.mType, PropertyContentType, prop); err != nil {
				return nil, err
			}
		}
	}
	return pkt, nil
}

// PublishID get publish ID to check No Local
func (msg *Publish) PublishID() uintptr {
	return msg.publishID
}

// SetPublishID internally used publish id to allow No Local option
func (msg *Publish) SetPublishID(id uintptr) {
	msg.publishID = id
}

// Set topic/payload/qos/retained/bool
func (msg *Publish) Set(t string, p []byte, q QosType, r bool, d bool) error {
	if !ValidTopic(t) {
		return ErrInvalidTopic
	}

	if !q.IsValid() {
		return ErrInvalidQoS
	}

	msg.mFlags &= ^maskPublishFlagQoS
	msg.mFlags |= byte(q) << 1
	msg.topic = t
	msg.payload = p
	if d {
		msg.mFlags |= maskPublishFlagDup
	} else {
		msg.mFlags &= ^maskPublishFlagDup
	}

	if r {
		msg.mFlags |= maskPublishFlagRetain
	} else {
		msg.mFlags &= ^maskPublishFlagRetain
	}

	return nil
}

// Dup returns the value specifying the duplicate delivery of a PUBLISH Control Packet.
// If the DUP flag is set to 0, it indicates that this is the first occasion that the
// Client or Server has attempted to send this MQTT PUBLISH Packet. If the DUP flag is
// set to 1, it indicates that this might be re-delivery of an earlier attempt to send
// the Packet.
func (msg *Publish) Dup() bool {
	return (msg.mFlags & maskPublishFlagDup) != 0
}

// SetDup sets the value specifying the duplicate delivery of a PUBLISH Control Packet.
func (msg *Publish) SetDup(v bool) {
	if v {
		msg.mFlags |= maskPublishFlagDup // 0x8 // 00001000
	} else {
		msg.mFlags &= ^maskPublishFlagDup // 247 // 11110111
	}
}

// Retain returns the value of the RETAIN flag. This flag is only used on the PUBLISH
// Packet. If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a
// Server, the Server MUST store the Application Message and its QoS, so that it can be
// delivered to future subscribers whose subscriptions match its topic name.
func (msg *Publish) Retain() bool {
	return (msg.mFlags & maskPublishFlagRetain) != 0
}

// SetRetain sets the value of the RETAIN flag.
func (msg *Publish) SetRetain(v bool) {
	if v {
		msg.mFlags |= maskPublishFlagRetain
	} else {
		msg.mFlags &= ^maskPublishFlagRetain
	}
}

// QoS returns the field that indicates the level of assurance for delivery of an
// Application Message. The values are QoS0, QoS1 and QoS2.
func (msg *Publish) QoS() QosType {
	return QosType((msg.mFlags & maskPublishFlagQoS) >> offsetPublishFlagQoS)
}

// SetQoS sets the field that indicates the level of assurance for delivery of an
// Application Message. The values are QoS0, QoS1 and QoS2.
// An error is returned if the value is not one of these.
func (msg *Publish) SetQoS(v QosType) error {
	if !v.IsValid() {
		return ErrInvalidQoS
	}

	msg.mFlags &= ^maskPublishFlagQoS
	msg.mFlags |= byte(v) << 1

	return nil
}

// Topic returns the the topic name that identifies the information channel to which
// payload data is published.
func (msg *Publish) Topic() string {
	return msg.topic
}

// SetTopic sets the the topic name that identifies the information channel to which
// payload data is published. An error is returned if ValidTopic() is falbase.
func (msg *Publish) SetTopic(v string) error {
	if (msg.version < ProtocolV50 && len(v) == 0) || !ValidTopic(v) {
		return ErrInvalidTopic
	}

	msg.topic = v

	return nil
}

// Payload returns the application message that's part of the PUBLISH message.
func (msg *Publish) Payload() []byte {
	return msg.payload
}

// SetPayload sets the application message that's part of the PUBLISH message.
func (msg *Publish) SetPayload(v []byte) {
	msg.payload = v
}

// SetPacketID sets the ID of the packet.
func (msg *Publish) SetPacketID(v IDType) {
	msg.setPacketID(v)
}

func (msg *Publish) decodeMessage(from []byte) (int, error) {
	var err error
	var n int
	var buf []byte
	offset := 0

	if !msg.QoS().IsValid() {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeProtocolError
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return offset, rejectCode
	}

	// [MQTT-3.3.1-2]
	if msg.QoS() == QoS0 && msg.Dup() {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeProtocolError
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return offset, rejectCode
	}

	// [MQTT-3.3.2.1]
	buf, n, err = ReadLPBytes(from[offset:])
	offset += n
	if err != nil {
		return offset, err
	}

	if len(buf) == 0 && msg.version < ProtocolV50 {
		return offset, CodeRefusedServerUnavailable
	} else if !ValidTopic(string(buf)) {
		rejectCode := CodeRefusedServerUnavailable
		if msg.version == ProtocolV50 {
			rejectCode = CodeInvalidTopicName
		}

		return offset, rejectCode
	}

	msg.topic = string(buf)

	// The packet identifier field is only present in the PUBLISH packets where the
	// QoS level is 1 or 2
	if msg.QoS() != QoS0 {
		offset += msg.decodePacketID(from[offset:])
	}

	if msg.version == ProtocolV50 {
		n, err = msg.properties.decode(msg.Type(), from[offset:])
		offset += n
		if err != nil {
			return offset, err
		}

		// if packet does not have topic set there must be topic alias set in properties
		if len(msg.topic) == 0 {
			reject := CodeProtocolError
			if prop := msg.PropertyGet(PropertyTopicAlias); prop != nil {
				if val, ok := prop.AsShort(); ok == nil && val > 0 {
					reject = CodeSuccess
				}
			}

			if reject != CodeSuccess {
				return offset, reject
			}
		}
	}

	pLen := int(msg.remLen) - offset

	// check payload len is not malformed
	if pLen < 0 {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeMalformedPacket
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return offset, rejectCode
	}

	// check payload is not malformed
	if len(from[offset:]) < pLen {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeMalformedPacket
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return offset, rejectCode
	}

	if pLen > 0 {
		msg.payload = make([]byte, pLen)
		copy(msg.payload, from[offset:offset+pLen])
		offset += pLen
	}

	return offset, nil
}

func (msg *Publish) encodeMessage(to []byte) (int, error) {
	if !ValidTopic(msg.topic) {
		return 0, ErrInvalidTopic
	}

	if !msg.QoS().IsValid() {
		return 0, ErrInvalidQoS
	}

	if msg.QoS() == QoS0 && msg.Dup() {
		return 0, ErrDupViolation
	}

	// [MQTT-2.3.1]
	if msg.QoS() != QoS0 && len(msg.packetID) == 0 {
		return 0, ErrPackedIDZero
	}

	var err error
	var n int
	offset := 0

	// [MQTT-3.3.2.1]
	if n, err = WriteLPBytes(to[offset:], []byte(msg.topic)); err != nil {
		return offset, err
	}
	offset += n

	// [MQTT-3.3.2.2]
	if msg.QoS() != QoS0 {
		offset += msg.encodePacketID(to[offset:])
	}

	// V5.0   [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		n, err = msg.properties.encode(to[offset:])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	offset += copy(to[offset:], msg.payload)

	return offset, nil
}

func (msg *Publish) size() int {
	total := 2 + len(msg.topic) + len(msg.payload)

	if msg.QoS() != 0 {
		// QoS1/2 packets must include packet id
		total += 2
	}

	// v5.0 [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		total += int(msg.properties.FullLen())
	}

	return total
}
