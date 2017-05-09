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

import "fmt"

// SubAckMessage A SUBACK Packet is sent by the Server to the Client to confirm receipt and processing
// of a SUBSCRIBE Packet.
//
// A SUBACK Packet contains a list of return codes, that specify the maximum QoS level
// that was granted in each Subscription that was requested by the SUBSCRIBE.
type SubAckMessage struct {
	header

	returnCodes []QosType
}

var _ Provider = (*SubAckMessage)(nil)

// NewSubAckMessage creates a new SUBACK message.
func NewSubAckMessage() *SubAckMessage {
	msg := &SubAckMessage{}
	msg.SetType(SUBACK) // nolint: errcheck

	return msg
}

// String returns a string representation of the message.
func (sam SubAckMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d, Return Codes=%v", sam.header, sam.PacketID(), sam.returnCodes)
}

// ReturnCodes returns the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
func (sam *SubAckMessage) ReturnCodes() []QosType {
	return sam.returnCodes
}

// AddReturnCodes sets the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
// An error is returned if any of the QoS values are not valid.
func (sam *SubAckMessage) AddReturnCodes(ret []QosType) error {
	for _, c := range ret {
		if !c.IsValidFull() {
			return ErrInvalidReturnCode
		}

		sam.returnCodes = append(sam.returnCodes, c)
	}

	sam.dirty = true

	return nil
}

// AddReturnCode adds a single QoS return value.
func (sam *SubAckMessage) AddReturnCode(ret QosType) error {
	return sam.AddReturnCodes([]QosType{ret})
}

// Len of message
func (sam *SubAckMessage) Len() int {
	if !sam.dirty {
		return len(sam.dBuf)
	}

	ml := sam.msgLen()

	if err := sam.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return sam.header.msgLen() + ml
}

// Decode message
func (sam *SubAckMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := sam.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	//this.packetID = binary.BigEndian.Uint16(src[total:])
	sam.packetID = src[total : total+2]
	total += 2

	l := int(sam.remLen) - (total - hn)
	//sam.returnCodes = []QosType(src[total : total+l])
	for q := range src[total : total+l] {
		sam.returnCodes = append(sam.returnCodes, QosType(q))
	}

	total += len(sam.returnCodes)

	for _, code := range sam.returnCodes {
		if !code.IsValidFull() {
			return total, ErrInvalidReturnCode
		}
	}

	sam.dirty = false

	return total, nil
}

// Encode message
func (sam *SubAckMessage) Encode(dst []byte) (int, error) {
	if !sam.dirty {
		if len(dst) < len(sam.dBuf) {
			return 0, ErrInsufficientBufferSize
		}

		return copy(dst, sam.dBuf), nil
	}

	for _, code := range sam.returnCodes {
		if !code.IsValidFull() {
			return 0, ErrInvalidReturnCode
		}
	}

	hl := sam.header.msgLen()
	ml := sam.msgLen()

	if len(dst) < hl+ml {
		return 0, ErrInsufficientBufferSize
	}

	if err := sam.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := sam.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	if copy(dst[total:total+2], sam.packetID) != 2 {
		dst[total], dst[total+1] = 0, 0
	}
	total += 2

	for i, q := range sam.returnCodes {
		dst[total+i] = byte(q)
	}
	//copy(dst[total:], []byte(sam.returnCodes))
	total += len(sam.returnCodes)

	return total, nil
}

func (sam *SubAckMessage) msgLen() int {
	return 2 + len(sam.returnCodes)
}
