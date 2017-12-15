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

// SubAck A SUBACK Packet is sent by the Server to the Client to confirm receipt and processing
// of a SUBSCRIBE Packet.
//
// A SUBACK Packet contains a list of return codes, that specify the maximum QoS level
// that was granted in each Subscription that was requested by the SUBSCRIBE.
type SubAck struct {
	header

	returnCodes []ReasonCode
}

var _ Provider = (*SubAck)(nil)

func newSubAck() *SubAck {
	return &SubAck{}
}

// NewSubAck creates a new SUBACK packet
func NewSubAck(v ProtocolVersion) *SubAck {
	p := newSubAck()
	p.init(SUBACK, v, p.size, p.encodeMessage, p.decodeMessage)
	return p
}

// ReturnCodes returns the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
func (msg *SubAck) ReturnCodes() []ReasonCode {
	return msg.returnCodes
}

// AddReturnCodes sets the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
// An error is returned if any of the QoS values are not valid.
func (msg *SubAck) AddReturnCodes(ret []ReasonCode) error {
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
func (msg *SubAck) AddReturnCode(ret ReasonCode) error {
	return msg.AddReturnCodes([]ReasonCode{ret})
}

// SetPacketID sets the ID of the packet.
func (msg *SubAck) SetPacketID(v IDType) {
	msg.setPacketID(v)
}

// decode message
func (msg *SubAck) decodeMessage(from []byte) (int, error) {
	offset := msg.decodePacketID(from)

	if msg.version == ProtocolV50 {
		n, err := msg.properties.decode(msg.Type(), from[offset:])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	numCodes := int(msg.remLen) - offset

	for i, q := range from[offset : offset+numCodes] {
		code := ReasonCode(q)
		if msg.version == ProtocolV50 && !code.IsValidForType(msg.mType) {
			return offset + i, CodeProtocolError
		} else if !QosType(code).IsValidFull() {
			return offset + i, CodeRefusedServerUnavailable
		}
		msg.returnCodes = append(msg.returnCodes, ReasonCode(q))
	}

	offset += numCodes

	return offset, nil
}

func (msg *SubAck) encodeMessage(to []byte) (int, error) {
	// [MQTT-2.3.1]
	if len(msg.packetID) == 0 {
		return 0, ErrPackedIDZero
	}

	offset := msg.encodePacketID(to)

	if msg.version == ProtocolV50 {
		n, err := msg.properties.encode(to[offset:])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	for _, q := range msg.returnCodes {
		to[offset] = byte(q)
		offset++
	}

	return offset, nil
}

func (msg *SubAck) size() int {
	total := 2 + len(msg.returnCodes)
	// v5.0 [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		total += int(msg.properties.FullLen())
	}

	return total
}
