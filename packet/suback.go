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
func (msg *SubAck) decodeMessage(src []byte) (int, error) {
	total := msg.decodePacketID(src)

	if msg.version == ProtocolV50 {
		var n int
		var err error
		if msg.properties, n, err = decodeProperties(msg.Type(), src[total:]); err != nil {
			return total + n, err
		}

		total += n
	}

	numCodes := int(msg.remLen) - total

	for i, q := range src[total : total+numCodes] {
		code := ReasonCode(q)
		if msg.version == ProtocolV50 && !code.IsValidForType(msg.mType) {
			return total + i, CodeProtocolError
		} else if !QosType(code).IsValidFull() {
			return total + i, CodeRefusedServerUnavailable
		}
		msg.returnCodes = append(msg.returnCodes, ReasonCode(q))
	}

	total += numCodes

	return total, nil
}

func (msg *SubAck) encodeMessage(dst []byte) (int, error) {
	// [MQTT-2.3.1]
	if len(msg.packetID) == 0 {
		return 0, ErrPackedIDZero
	}

	total := msg.encodePacketID(dst)

	if msg.version == ProtocolV50 {
		var n int
		var err error
		if n, err = encodeProperties(msg.properties, dst[total:]); err != nil {
			return total + n, err
		}

		total += n
	}

	for _, q := range msg.returnCodes {
		dst[total] = byte(q)
		total++
	}

	return total, nil
}

func (msg *SubAck) size() int {
	total := 2 + len(msg.returnCodes)
	// v5.0 [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		pLen, _ := encodeProperties(msg.properties, []byte{})
		total += pLen
	}

	return total
}
