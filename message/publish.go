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
	"fmt"
	"github.com/troian/surgemq/buffer"
	"sync/atomic"
)

const (
	publishFlagDupMask    byte = 0x08
	publishFlagQosMask    byte = 0x06
	publishFlagRetainMask byte = 0x01
)

// PublishMessage A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client
// to transport an Application Message.
type PublishMessage struct {
	header

	topic   string
	payload []byte
}

var _ Provider = (*PublishMessage)(nil)

// NewPublishMessage creates a new PUBLISH message.
func NewPublishMessage() *PublishMessage {
	msg := &PublishMessage{}
	msg.SetType(PUBLISH) // nolint: errcheck

	return msg
}

func (msg *PublishMessage) String() string {
	return fmt.Sprintf("%s, Topic=%q, Packet ID=%d, QoS=%d, Retained=%t, Dup=%t, Payload=%v",
		msg.header, msg.topic, msg.packetID, msg.QoS(), msg.Retain(), msg.Dup(), msg.payload)
}

// Dup returns the value specifying the duplicate delivery of a PUBLISH Control Packet.
// If the DUP flag is set to 0, it indicates that this is the first occasion that the
// Client or Server has attempted to send this MQTT PUBLISH Packet. If the DUP flag is
// set to 1, it indicates that this might be re-delivery of an earlier attempt to send
// the Packet.
func (msg *PublishMessage) Dup() bool {
	return (msg.Flags() & publishFlagDupMask) != 0
}

// SetDup sets the value specifying the duplicate delivery of a PUBLISH Control Packet.
func (msg *PublishMessage) SetDup(v bool) {
	if v {
		msg.mTypeFlags[0] |= publishFlagDupMask // 0x8 // 00001000
	} else {
		msg.mTypeFlags[0] &= ^publishFlagDupMask // 247 // 11110111
	}
}

// Retain returns the value of the RETAIN flag. This flag is only used on the PUBLISH
// Packet. If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a
// Server, the Server MUST store the Application Message and its QoS, so that it can be
// delivered to future subscribers whose subscriptions match its topic name.
func (msg *PublishMessage) Retain() bool {
	return (msg.Flags() & publishFlagRetainMask) != 0
}

// SetRetain sets the value of the RETAIN flag.
func (msg *PublishMessage) SetRetain(v bool) {
	if v {
		msg.mTypeFlags[0] |= publishFlagRetainMask //0x1 // 00000001
	} else {
		msg.mTypeFlags[0] &= ^publishFlagRetainMask // 254 // 11111110
	}
}

// QoS returns the field that indicates the level of assurance for delivery of an
// Application Message. The values are QosAtMostOnce, QosAtLeastOnce and QosExactlyOnce.
func (msg *PublishMessage) QoS() QosType {
	return QosType((msg.Flags() & publishFlagQosMask) >> 1)
}

// SetQoS sets the field that indicates the level of assurance for delivery of an
// Application Message. The values are QosAtMostOnce, QosAtLeastOnce and QosExactlyOnce.
// An error is returned if the value is not one of these.
func (msg *PublishMessage) SetQoS(v QosType) error {
	if !v.IsValid() {
		return ErrInvalidQoS
	}
	msg.mTypeFlags[0] &= ^publishFlagQosMask

	msg.mTypeFlags[0] |= byte(v) << 1 // (msg.mTypeFlags[0] & 249) | byte(v<<1) // 249 = 11111001

	return nil
}

// Topic returns the the topic name that identifies the information channel to which
// payload data is published.
func (msg *PublishMessage) Topic() string {
	return msg.topic
}

// SetTopic sets the the topic name that identifies the information channel to which
// payload data is published. An error is returned if ValidTopic() is falbase.
func (msg *PublishMessage) SetTopic(v string) error {
	if !ValidTopic(v) {
		return ErrInvalidTopic
	}

	msg.topic = v
	msg.dirty = true

	return nil
}

// Payload returns the application message that's part of the PUBLISH message.
func (msg *PublishMessage) Payload() []byte {
	return msg.payload
}

// SetPayload sets the application message that's part of the PUBLISH message.
func (msg *PublishMessage) SetPayload(v []byte) {
	msg.payload = v
	msg.dirty = true
}

// Len of message
func (msg *PublishMessage) Len() int {
	if !msg.dirty {
		return len(msg.dBuf)
	}

	ml := msg.msgLen()

	if err := msg.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return msg.header.msgLen() + ml
}

// Decode message
func (msg *PublishMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := msg.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	var n int
	var buf []byte
	buf, n, err = readLPBytes(src[total:])
	msg.topic = string(buf)
	total += n
	if err != nil {
		return total, err
	}

	if !ValidTopic(msg.topic) {
		return total, ErrInvalidTopic
	}

	// The packet identifier field is only present in the PUBLISH packets where the
	// QoS level is 1 or 2
	if msg.QoS() != 0 {
		//msg.packetId = binary.BigEndian.Uint16(src[total:])
		msg.packetID = src[total : total+2]
		total += 2
	}

	l := int(msg.remLen) - (total - hn)
	msg.payload = src[total : total+l]
	total += len(msg.payload)

	msg.dirty = false

	return total, nil
}

// Encode message
func (msg *PublishMessage) Encode(dst []byte) (int, error) {
	expectedSize := msg.Len()
	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	var err error
	total := 0

	if !msg.dirty {
		total = copy(dst, msg.dBuf)
	} else {
		if len(msg.topic) == 0 {
			return 0, ErrInvalidTopic
		}

		if len(msg.payload) == 0 {
			return 0, ErrEmptyPayload
		}

		var n int

		if n, err = msg.header.encode(dst[total:]); err != nil {
			return total, err
		}
		total += n

		if n, err = writeLPBytes(dst[total:], []byte(msg.topic)); err != nil {
			return total, err
		}
		total += n

		// The packet identifier field is only present in the PUBLISH packets where the QoS level is 1 or 2
		if msg.QoS() == QosAtLeastOnce || msg.QoS() == QosExactlyOnce {
			if msg.PacketID() == 0 {
				msg.SetPacketID(uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff))
			}

			copy(dst[total:], msg.packetID)
			total += 2
		}

		total += copy(dst[total:], msg.payload)
	}

	return total, err
}

// Send encode and send message into ring buffer
func (msg *PublishMessage) Send(to *buffer.Type) (int, error) {
	var err error
	total := 0

	if !msg.dirty {
		total, err = to.Send(msg.dBuf)
	} else {
		if len(msg.topic) == 0 {
			return 0, ErrInvalidTopic
		}

		if len(msg.payload) == 0 {
			return 0, ErrEmptyPayload
		}

		expectedSize := msg.Len()
		if len(to.ExternalBuf) < expectedSize {
			to.ExternalBuf = make([]byte, expectedSize)
		}

		var n int

		if n, err = msg.header.encode(to.ExternalBuf[total:]); err != nil {
			return total, err
		}
		total += n

		if n, err = writeLPBytes(to.ExternalBuf[total:], []byte(msg.topic)); err != nil {
			return total, err
		}
		total += n

		// The packet identifier field is only present in the PUBLISH packets where the QoS level is 1 or 2
		if msg.QoS() == QosAtLeastOnce || msg.QoS() == QosExactlyOnce {
			if msg.PacketID() == 0 {
				msg.SetPacketID(uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff))
			}

			copy(to.ExternalBuf[total:total+2], msg.packetID)
			total += 2
		}

		if n, err = to.Send(to.ExternalBuf[:total]); err != nil {
			return 0, err
		}
		total = n

		if n, err = to.Send(msg.payload); err != nil {
			return 0, err
		}

		total += n

		return total, nil

	}

	return total, err
}

func (msg *PublishMessage) msgLen() int {
	total := 2 + len(msg.topic) + len(msg.payload)
	if msg.QoS() != 0 {
		total += 2
	}

	return total
}
