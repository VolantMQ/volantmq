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

// A SUBACK Packet is sent by the Server to the Client to confirm receipt and processing
// of a SUBSCRIBE Packet.
//
// A SUBACK Packet contains a list of return codes, that specify the maximum QoS level
// that was granted in each Subscription that was requested by the SUBSCRIBE.
type SubAckMessage struct {
	header

	returnCodes []byte
}

var _ Message = (*SubAckMessage)(nil)

// NewSubAckMessage creates a new SUBACK message.
func NewSubAckMessage() *SubAckMessage {
	msg := &SubAckMessage{}
	msg.SetType(SUBACK)

	return msg
}

// String returns a string representation of the message.
func (sam SubAckMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d, Return Codes=%v", sam.header, sam.PacketId(), sam.returnCodes)
}

// ReturnCodes returns the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
func (sam *SubAckMessage) ReturnCodes() []byte {
	return sam.returnCodes
}

// AddReturnCodes sets the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
// An error is returned if any of the QoS values are not valid.
func (sam *SubAckMessage) AddReturnCodes(ret []byte) error {
	for _, c := range ret {
		if c != QosAtMostOnce && c != QosAtLeastOnce && c != QosExactlyOnce && c != QosFailure {
			return fmt.Errorf("suback/AddReturnCode: Invalid return code %d. Must be 0, 1, 2, 0x80.", c)
		}

		sam.returnCodes = append(sam.returnCodes, c)
	}

	sam.dirty = true

	return nil
}

// AddReturnCode adds a single QoS return value.
func (sam *SubAckMessage) AddReturnCode(ret byte) error {
	return sam.AddReturnCodes([]byte{ret})
}

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
	sam.returnCodes = src[total : total+l]
	total += len(sam.returnCodes)

	for i, code := range sam.returnCodes {
		if code != 0x00 && code != 0x01 && code != 0x02 && code != 0x80 {
			return total, fmt.Errorf("suback/Decode: Invalid return code %d for topic %d", code, i)
		}
	}

	sam.dirty = false

	return total, nil
}

func (sam *SubAckMessage) Encode(dst []byte) (int, error) {
	if !sam.dirty {
		if len(dst) < len(sam.dBuf) {
			return 0, fmt.Errorf("suback/Encode: Insufficient buffer size. Expecting %d, got %d.", len(sam.dBuf), len(dst))
		}

		return copy(dst, sam.dBuf), nil
	}

	for i, code := range sam.returnCodes {
		if code != 0x00 && code != 0x01 && code != 0x02 && code != 0x80 {
			return 0, fmt.Errorf("suback/Encode: Invalid return code %d for topic %d", code, i)
		}
	}

	hl := sam.header.msgLen()
	ml := sam.msgLen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("suback/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
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

	copy(dst[total:], sam.returnCodes)
	total += len(sam.returnCodes)

	return total, nil
}

func (sam *SubAckMessage) msgLen() int {
	return 2 + len(sam.returnCodes)
}
