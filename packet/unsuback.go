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

// UnSubAck The UNSUBACK Packet is sent by the Server to the Client to confirm receipt of an
// UNSUBSCRIBE Packet.
type UnSubAck struct {
	header

	returnCodes []ReasonCode
}

var _ Provider = (*UnSubAck)(nil)

func newUnSubAck() *UnSubAck {
	msg := &UnSubAck{}

	return msg
}

// NewUnSubAck creates a new UNSUBACK packet
func NewUnSubAck(v ProtocolVersion) *UnSubAck {
	p := newUnSubAck()
	p.init(UNSUBACK, v, p.size, p.encodeMessage, p.decodeMessage)
	return p
}

// SetPacketID sets the ID of the packet.
func (msg *UnSubAck) SetPacketID(v IDType) {
	msg.setPacketID(v)
}

// ReturnCodes returns the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
func (msg *UnSubAck) ReturnCodes() []ReasonCode {
	return msg.returnCodes
}

// AddReturnCodes sets the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
// An error is returned if any of the QoS values are not valid.
func (msg *UnSubAck) AddReturnCodes(ret []ReasonCode) error {
	for _, c := range ret {
		if msg.version == ProtocolV50 && !c.IsValidForType(msg.mType) {
			return ErrInvalidReturnCode
		} else if !QosType(c).IsValidFull() {
			return ErrInvalidReturnCode
		}

		msg.returnCodes = append(msg.returnCodes, c)
	}

	return nil
}

// AddReturnCode adds a single QoS return value.
func (msg *UnSubAck) AddReturnCode(ret ReasonCode) error {
	return msg.AddReturnCodes([]ReasonCode{ret})
}

// decode message
func (msg *UnSubAck) decodeMessage(from []byte) (int, error) {
	offset := msg.decodePacketID(from)

	if msg.version == ProtocolV50 && (int(msg.remLen)-offset) > 0 {
		n, err := msg.properties.decode(msg.Type(), from[offset:])
		offset += n
		if err != nil {
			return offset, err
		}

		for _, c := range from[offset:msg.remLen] {
			msg.returnCodes = append(msg.returnCodes, ReasonCode(c))
		}
	}

	return offset, nil
}

func (msg *UnSubAck) encodeMessage(to []byte) (int, error) {
	// [MQTT-2.3.1]
	if len(msg.packetID) == 0 {
		return 0, ErrPackedIDZero
	}

	offset := msg.encodePacketID(to)

	var err error
	if msg.version == ProtocolV50 {
		var n int
		n, err = msg.properties.encode(to[offset:])
		offset += n

		for _, c := range msg.returnCodes {
			to[offset] = byte(c)
			offset++
		}
	}

	return offset, err
}

func (msg *UnSubAck) size() int {
	// packet ID
	total := 2

	if msg.version == ProtocolV50 {
		total += int(msg.properties.FullLen())
		total += len(msg.returnCodes)
	}

	return total
}
