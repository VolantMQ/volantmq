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

package packet

// Publish A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client
// to transport an Application Message.
type Publish struct {
	header

	payload []byte
	topic   string
}

var _ Provider = (*Publish)(nil)

func newPublish() *Publish {
	return &Publish{}
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
	if !ValidTopic(v) {
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

func (msg *Publish) decodeMessage(src []byte) (int, error) {
	var err error
	var n int
	var buf []byte
	total := 0

	if !msg.QoS().IsValid() {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeProtocolError
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return total, rejectCode
	}

	// [MQTT-3.3.1-2]
	if msg.QoS() == QoS0 && msg.Dup() {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeProtocolError
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return total, rejectCode
	}

	buf, n, err = ReadLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	if !ValidTopic(string(buf)) {
		return total, ErrInvalidTopic
	}

	msg.topic = string(buf)

	// The packet identifier field is only present in the PUBLISH packets where the
	// QoS level is 1 or 2
	if msg.QoS() != QoS0 {
		total += msg.decodePacketID(src[total:])
	}

	if msg.version == ProtocolV50 {
		msg.properties, n, err = decodeProperties(msg.Type(), buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	pLen := int(msg.remLen) - total

	// check payload len is not malformed
	if pLen < 0 {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeMalformedPacket
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return total, rejectCode
	}

	// check payload is not malformed
	if len(src[total:]) < pLen {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeMalformedPacket
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return total, rejectCode
	}

	if pLen > 0 {
		msg.payload = make([]byte, pLen)
		copy(msg.payload, src[total:total+pLen])
		total += pLen
	}

	return total, nil
}

func (msg *Publish) encodeMessage(dst []byte) (int, error) {
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
	total := 0

	if n, err = WriteLPBytes(dst[total:], []byte(msg.topic)); err != nil {
		return total, err
	}
	total += n

	if msg.QoS() != QoS0 {
		total += msg.encodePacketID(dst[total:])
	}

	// V5.0   [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		if n, err = encodeProperties(msg.properties, dst[total:]); err != nil {
			return total + n, err
		}

		total += n
	}

	total += copy(dst[total:], msg.payload)

	return total, nil
}

func (msg *Publish) size() int {
	total := 2 + len(msg.topic) + len(msg.payload)

	if msg.QoS() != 0 {
		total += 2
	}

	// v5.0 [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		pLen, _ := encodeProperties(msg.properties, []byte{})
		total += pLen
	}

	return total
}
