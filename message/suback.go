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
	"encoding/binary"
	"fmt"

	"github.com/troian/surgemq/buffer"
)

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
func (msg *SubAckMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d, Return Codes=%v", msg.header, msg.packetID, msg.returnCodes)
}

// ReturnCodes returns the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
func (msg *SubAckMessage) ReturnCodes() []QosType {
	return msg.returnCodes
}

// AddReturnCodes sets the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
// An error is returned if any of the QoS values are not valid.
func (msg *SubAckMessage) AddReturnCodes(ret []QosType) error {
	for _, c := range ret {
		if !c.IsValidFull() {
			return ErrInvalidReturnCode
		}

		msg.returnCodes = append(msg.returnCodes, c)
	}

	return nil
}

// AddReturnCode adds a single QoS return value.
func (msg *SubAckMessage) AddReturnCode(ret QosType) error {
	return msg.AddReturnCodes([]QosType{ret})
}

// SetPacketID sets the ID of the packet.
func (msg *SubAckMessage) SetPacketID(v uint16) {
	msg.packetID = v
}

// Len of message
func (msg *SubAckMessage) Len() int {
	ml := msg.msgLen()

	if err := msg.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return msg.header.msgLen() + ml
}

// Decode message
func (msg *SubAckMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := msg.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	msg.packetID = binary.BigEndian.Uint16(src[total:])
	total += 2

	l := int(msg.remLen) - (total - hn)

	if len(msg.returnCodes) < l {
		msg.returnCodes = make([]QosType, l)
	}
	for i, q := range src[total : total+l] {
		msg.returnCodes[i] = QosType(q)
	}

	total += len(msg.returnCodes)

	for _, code := range msg.returnCodes {
		if !code.IsValidFull() {
			return total, ErrInvalidReturnCode
		}
	}

	return total, nil
}

func (msg *SubAckMessage) preEncode(dst []byte) (int, error) {
	// [MQTT-2.3.1]
	if msg.packetID == 0 {
		return 0, ErrPackedIDZero
	}

	var err error
	total := 0

	var n int

	if n, err = msg.header.encode(dst[total:]); err != nil {
		return total, err
	}
	total += n

	binary.BigEndian.PutUint16(dst[total:], msg.packetID)
	total += 2
	for i, q := range msg.returnCodes {
		dst[total+i] = byte(q)
		total++
	}

	return total, err
}

// Encode message
func (msg *SubAckMessage) Encode(dst []byte) (int, error) {
	expectedSize := msg.Len()
	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	return msg.preEncode(dst)
}

// Send encode and send message into ring buffer
func (msg *SubAckMessage) Send(to *buffer.Type) (int, error) {
	expectedSize := msg.Len()
	if len(to.ExternalBuf) < expectedSize {
		to.ExternalBuf = make([]byte, expectedSize)
	}

	total, err := msg.preEncode(to.ExternalBuf)
	if err != nil {
		return 0, err
	}

	return to.Send([][]byte{to.ExternalBuf[:total]})
}

func (msg *SubAckMessage) msgLen() int {
	return 2 + len(msg.returnCodes)
}
