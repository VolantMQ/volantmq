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
	return fmt.Sprintf("%s, Packet ID=%d, Return Codes=%v", msg.header, msg.PacketID(), msg.returnCodes)
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

	msg.dirty = true

	return nil
}

// AddReturnCode adds a single QoS return value.
func (msg *SubAckMessage) AddReturnCode(ret QosType) error {
	return msg.AddReturnCodes([]QosType{ret})
}

// Len of message
func (msg *SubAckMessage) Len() int {
	if !msg.dirty {
		return len(msg.dBuf)
	}

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

	//this.packetID = binary.BigEndian.Uint16(src[total:])
	msg.packetID = src[total : total+2]
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

	msg.dirty = false

	return total, nil
}

// Encode message
func (msg *SubAckMessage) Encode(dst []byte) (int, error) {
	expectedSize := msg.Len()
	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	var err error
	total := 0

	if !msg.dirty {
		total = copy(dst, msg.dBuf)
	} else {
		var n int

		if n, err = msg.header.encode(dst[total:]); err != nil {
			return total, err
		}
		total += n

		if copy(dst[total:total+2], msg.packetID) != 2 {
			dst[total] = 0
			dst[total+1] = 0
		}
		total += 2

		for i, q := range msg.returnCodes {
			dst[total+i] = byte(q)
			total++
		}
	}

	return total, err
}

// Send encode and send message into ring buffer
func (msg *SubAckMessage) Send(to *buffer.Type) (int, error) {
	var err error
	total := 0

	if !msg.dirty {
		total, err = to.Send(msg.dBuf)
	} else {
		expectedSize := msg.Len()
		if len(to.ExternalBuf) < expectedSize {
			to.ExternalBuf = make([]byte, expectedSize)
		}

		var n int

		if n, err = msg.header.encode(to.ExternalBuf[total:]); err != nil {
			return 0, err
		}
		total += n

		if copy(to.ExternalBuf[total:total+2], msg.packetID) != 2 {
			to.ExternalBuf[total] = 0
			to.ExternalBuf[total+1] = 0
		}

		total += 2

		for i, q := range msg.returnCodes {
			to.ExternalBuf[total+i] = byte(q)
		}
		total += len(msg.returnCodes)

		total, err = to.Send(to.ExternalBuf[:total])
	}

	return total, err
}

func (msg *SubAckMessage) msgLen() int {
	return 2 + len(msg.returnCodes)
}
