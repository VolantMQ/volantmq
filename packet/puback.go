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

// NewPubAck creates a new PUBACK packet
func NewPubAck(v ProtocolVersion) *Ack {
	p := newPubAck()
	p.init(PUBACK, v, p.size, p.encodeMessage, p.decodeMessage)
	return p
}

// NewPubRec creates a new PUBREC packet
func NewPubRec(v ProtocolVersion) *Ack {
	p := newPubAck()
	p.init(PUBREC, v, p.size, p.encodeMessage, p.decodeMessage)
	return p
}

// NewPubRel creates a new PUBREL packet
func NewPubRel(v ProtocolVersion) *Ack {
	p := newPubAck()
	p.init(PUBREL, v, p.size, p.encodeMessage, p.decodeMessage)
	return p
}

// NewPubComp creates a new PUBCOMP packet
func NewPubComp(v ProtocolVersion) *Ack {
	p := newPubAck()
	p.init(PUBCOMP, v, p.size, p.encodeMessage, p.decodeMessage)
	return p
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

func (msg *Ack) decodeMessage(from []byte) (int, error) {
	offset := msg.decodePacketID(from)

	if msg.version == ProtocolV50 {
		// [MQTT-3.4.2.1]
		if len(from[offset:]) == 0 {
			msg.reasonCode = CodeSuccess
			return offset, nil
		}

		msg.reasonCode = ReasonCode(from[offset])
		if !msg.reasonCode.IsValidForType(msg.mType) {
			return offset, CodeMalformedPacket
		}
		offset++

		if len(from[offset:]) > 0 {
			// v5 [MQTT-3.1.2.11] specifies properties in variable header
			n, err := msg.properties.decode(msg.Type(), from[offset:])
			offset += n
			if err != nil {
				return offset, err
			}
		}
	}

	return offset, nil
}

func (msg *Ack) encodeMessage(to []byte) (int, error) {
	// [MQTT-2.3.1]
	if len(msg.packetID) == 0 {
		return 0, ErrPackedIDZero
	}

	offset := msg.encodePacketID(to)

	var err error
	if msg.version == ProtocolV50 {
		pLen := msg.properties.FullLen()
		if pLen > 1 || msg.reasonCode != CodeSuccess {
			to[offset] = byte(msg.reasonCode)
			offset++

			if pLen > 1 {
				var n int
				n, err = msg.properties.encode(to[offset:])
				offset += n
			}
		}
	}

	return offset, err
}

func (msg *Ack) size() int {
	// include size of PacketID
	total := 2

	if msg.version == ProtocolV50 {
		pLen := msg.properties.FullLen()
		// If properties exist (which indicated when pLen > 1) include in body size reason code and properties
		// otherwise include only reason code if it differs from CodeSuccess
		if pLen > 1 || msg.reasonCode != CodeSuccess {
			total++
			if pLen > 1 {
				total += int(pLen)
			}
		}
	}

	return total
}
