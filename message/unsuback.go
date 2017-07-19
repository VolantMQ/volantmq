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

// UnSubAckMessage The UNSUBACK Packet is sent by the Server to the Client to confirm receipt of an
// UNSUBSCRIBE Packet.
type UnSubAckMessage struct {
	header

	returnCodes []ReasonCode
}

var _ Provider = (*UnSubAckMessage)(nil)

func newUnSubAckMessage() *UnSubAckMessage {
	msg := &UnSubAckMessage{}

	return msg
}

// SetPacketID sets the ID of the packet.
func (msg *UnSubAckMessage) SetPacketID(v PacketID) {
	msg.setPacketID(v)
}

// ReturnCodes returns the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
func (msg *UnSubAckMessage) ReturnCodes() []ReasonCode {
	return msg.returnCodes
}

// AddReturnCodes sets the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
// An error is returned if any of the QoS values are not valid.
func (msg *UnSubAckMessage) AddReturnCodes(ret []ReasonCode) error {
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
func (msg *UnSubAckMessage) AddReturnCode(ret ReasonCode) error {
	return msg.AddReturnCodes([]ReasonCode{ret})
}

// decode message
func (msg *UnSubAckMessage) decodeMessage(src []byte) (int, error) {
	total := msg.decodePacketID(src)

	if msg.version == ProtocolV50 && (int(msg.remLen)-total) > 0 {
		var n int
		var err error
		if msg.properties, n, err = decodeProperties(msg.Type(), src[total:]); err != nil {
			return total + n, err
		}

		total += n
	}

	return total, nil
}

func (msg *UnSubAckMessage) encodeMessage(dst []byte) (int, error) {
	// [MQTT-2.3.1]
	if len(msg.packetID) == 0 {
		return 0, ErrPackedIDZero
	}

	total := msg.encodePacketID(dst)

	if msg.version == ProtocolV50 {
		var n int
		var err error

		if n, err = encodeProperties(msg.properties, []byte{}); err != nil {
			return total, err
		}

		if n > 1 {
			if n, err = encodeProperties(msg.properties, dst[total:]); err != nil {
				return total + n, err
			}
			total += n
		}
	}

	return total, nil
}

func (msg *UnSubAckMessage) size() int {
	// packet ID
	total := 2

	if msg.version == ProtocolV50 {
		pLen, _ := encodeProperties(msg.properties, []byte{})
		total += pLen

		if pLen > 1 {
			total += pLen
		}
	}

	return total
}
