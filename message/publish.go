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
	"errors"
	"fmt"
	"sync/atomic"
)

// PublishMessage A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client
// to transport an Application Message.
type PublishMessage struct {
	header

	topic   string
	payload []byte
}

var _ Message = (*PublishMessage)(nil)

// NewPublishMessage creates a new PUBLISH message.
func NewPublishMessage() *PublishMessage {
	msg := &PublishMessage{}
	msg.SetType(PUBLISH) // nolint: errcheck

	return msg
}

func (pm PublishMessage) String() string {
	return fmt.Sprintf("%s, Topic=%q, Packet ID=%d, QoS=%d, Retained=%t, Dup=%t, Payload=%v",
		pm.header, pm.topic, pm.packetID, pm.QoS(), pm.Retain(), pm.Dup(), pm.payload)
}

// Dup returns the value specifying the duplicate delivery of a PUBLISH Control Packet.
// If the DUP flag is set to 0, it indicates that this is the first occasion that the
// Client or Server has attempted to send this MQTT PUBLISH Packet. If the DUP flag is
// set to 1, it indicates that this might be re-delivery of an earlier attempt to send
// the Packet.
func (pm *PublishMessage) Dup() bool {
	return ((pm.Flags() >> 3) & 0x1) == 1
}

// SetDup sets the value specifying the duplicate delivery of a PUBLISH Control Packet.
func (pm *PublishMessage) SetDup(v bool) {
	if v {
		pm.mTypeFlags[0] |= 0x8 // 00001000
	} else {
		pm.mTypeFlags[0] &= 247 // 11110111
	}
}

// Retain returns the value of the RETAIN flag. This flag is only used on the PUBLISH
// Packet. If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a
// Server, the Server MUST store the Application Message and its QoS, so that it can be
// delivered to future subscribers whose subscriptions match its topic name.
func (pm *PublishMessage) Retain() bool {
	return (pm.Flags() & 0x1) == 1
}

// SetRetain sets the value of the RETAIN flag.
func (pm *PublishMessage) SetRetain(v bool) {
	if v {
		pm.mTypeFlags[0] |= 0x1 // 00000001
	} else {
		pm.mTypeFlags[0] &= 254 // 11111110
	}
}

// QoS returns the field that indicates the level of assurance for delivery of an
// Application Message. The values are QosAtMostOnce, QosAtLeastOnce and QosExactlyOnce.
func (pm *PublishMessage) QoS() byte {
	return (pm.Flags() >> 1) & 0x3
}

// SetQoS sets the field that indicates the level of assurance for delivery of an
// Application Message. The values are QosAtMostOnce, QosAtLeastOnce and QosExactlyOnce.
// An error is returned if the value is not one of these.
func (pm *PublishMessage) SetQoS(v byte) error {
	if v != 0x0 && v != 0x1 && v != 0x2 {
		return fmt.Errorf("publish/SetQoS: Invalid QoS %d", v)
	}

	pm.mTypeFlags[0] = (pm.mTypeFlags[0] & 249) | (v << 1) // 249 = 11111001

	return nil
}

// Topic returns the the topic name that identifies the information channel to which
// payload data is published.
func (pm *PublishMessage) Topic() string {
	return pm.topic
}

// SetTopic sets the the topic name that identifies the information channel to which
// payload data is published. An error is returned if ValidTopic() is falbase.
func (pm *PublishMessage) SetTopic(v string) error {
	if !ValidTopic(v) {
		return fmt.Errorf("publish/SetTopic: Invalid topic name (%s). Must not be empty or contain wildcard characters", v)
	}

	pm.topic = v
	pm.dirty = true

	return nil
}

// Payload returns the application message that's part of the PUBLISH message.
func (pm *PublishMessage) Payload() []byte {
	return pm.payload
}

// SetPayload sets the application message that's part of the PUBLISH message.
func (pm *PublishMessage) SetPayload(v []byte) {
	pm.payload = v
	pm.dirty = true
}

// Len of message
func (pm *PublishMessage) Len() int {
	if !pm.dirty {
		return len(pm.dBuf)
	}

	ml := pm.msgLen()

	if err := pm.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return pm.header.msgLen() + ml
}

// Decode message
func (pm *PublishMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := pm.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	var n int
	var buf []byte
	buf, n, err = readLPBytes(src[total:])
	pm.topic = string(buf)
	total += n
	if err != nil {
		return total, err
	}

	if !ValidTopic(pm.topic) {
		return total, fmt.Errorf("publish/Decode: Invalid topic name (%s). Must not be empty or contain wildcard characters", pm.topic)
	}

	// The packet identifier field is only present in the PUBLISH packets where the
	// QoS level is 1 or 2
	if pm.QoS() != 0 {
		//pm.packetId = binary.BigEndian.Uint16(src[total:])
		pm.packetID = src[total : total+2]
		total += 2
	}

	l := int(pm.remLen) - (total - hn)
	pm.payload = src[total : total+l]
	total += len(pm.payload)

	pm.dirty = false

	return total, nil
}

// Encode message
func (pm *PublishMessage) Encode(dst []byte) (int, error) {
	if !pm.dirty {
		if len(dst) < len(pm.dBuf) {
			return 0, fmt.Errorf("publish/Encode: Insufficient buffer size. Expecting %d, got %d", len(pm.dBuf), len(dst))
		}

		return copy(dst, pm.dBuf), nil
	}

	if len(pm.topic) == 0 {
		return 0, errors.New("publish/Encode: Topic name is empty")
	}

	if len(pm.payload) == 0 {
		return 0, errors.New("publish/Encode: Payload is empty")
	}

	ml := pm.msgLen()

	if err := pm.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	hl := pm.header.msgLen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("publish/Encode: Insufficient buffer size. Expecting %d, got %d", hl+ml, len(dst))
	}

	total := 0

	n, err := pm.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	n, err = writeLPBytes(dst[total:], []byte(pm.topic))
	total += n
	if err != nil {
		return total, err
	}

	// The packet identifier field is only present in the PUBLISH packets where the QoS level is 1 or 2
	if pm.QoS() != 0 {
		if pm.PacketID() == 0 {
			pm.SetPacketID(uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff))
			//this.packetId = uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff)
		}

		n = copy(dst[total:], pm.packetID)
		//binary.BigEndian.PutUint16(dst[total:], this.packetId)
		total += n
	}

	copy(dst[total:], pm.payload)
	total += len(pm.payload)

	return total, nil
}

func (pm *PublishMessage) msgLen() int {
	total := 2 + len(pm.topic) + len(pm.payload)
	if pm.QoS() != 0 {
		total += 2
	}

	return total
}
