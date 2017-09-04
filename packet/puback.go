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

// Ack acknowledge packets for PUBLISH messages
// A PUBACK Packet is the response to a PUBLISH Packet with QoS level 1
// A PUBREC/PUBREL/PUBCOMP Packet is the response to a PUBLISH Packet with QoS level 2
type Ack struct {
	header

	reasonCode ReasonCode
}

var _ Provider = (*Ack)(nil)

func newPubAck() *Ack {
	return &Ack{}
}

func newPubRec() *Ack {
	return &Ack{}
}

func newPubRel() *Ack {
	return &Ack{}
}

func newPubComp() *Ack {
	return &Ack{}
}

// SetPacketID sets the ID of the packet.
func (msg *Ack) SetPacketID(v IDType) {
	msg.setPacketID(v)
}

// SetReason of acknowledgment
func (msg *Ack) SetReason(c ReasonCode) {
	if msg.version != ProtocolV50 {
		return
	}

	msg.reasonCode = c
}

// Reason return acknowledgment reason
func (msg *Ack) Reason() ReasonCode {
	return msg.reasonCode
}

func (msg *Ack) decodeMessage(src []byte) (int, error) {
	total := 0

	total += msg.decodePacketID(src[total:])

	if msg.version == ProtocolV50 {
		msg.reasonCode = ReasonCode(src[total])
		if !msg.reasonCode.IsValidForType(msg.mType) {
			return total, CodeMalformedPacket
		}
		total++

		// v5 [MQTT-3.1.2.11] specifies properties in variable header
		var err error
		var n int
		if msg.properties, n, err = decodeProperties(msg.mType, src[total:]); err != nil {
			return total + n, err
		}
		total += n
	}

	return total, nil
}

func (msg *Ack) encodeMessage(dst []byte) (int, error) {
	// [MQTT-2.3.1]
	if len(msg.packetID) == 0 {
		return 0, ErrPackedIDZero
	}

	total := 0

	total += msg.encodePacketID(dst[total:])

	if msg.version == ProtocolV50 {
		if !msg.reasonCode.IsValidForType(msg.mType) {
			return total, ErrInvalidReturnCode
		}

		dst[total] = byte(msg.reasonCode)
		total++

		// v5 [MQTT-3.1.2.11] specifies properties in variable header
		var err error
		var n int
		if n, err = encodeProperties(msg.properties, dst[total:]); err != nil {
			return total + n, err
		}

		total += n
	}

	return total, nil
}

func (msg *Ack) size() int {
	total := 2

	if msg.version == ProtocolV50 {
		// V5.0 [MQTT-3.4.2.1]
		total++

		// v5.0 [MQTT-3.1.2.11]
		pLen, _ := encodeProperties(msg.properties, []byte{})
		total += pLen
	}

	return total
}
