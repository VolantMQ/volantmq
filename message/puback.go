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

// PubAckMessage A PUBACK Packet is the response to a PUBLISH Packet with QoS level 1.
type PubAckMessage struct {
	header
}

var _ Provider = (*PubAckMessage)(nil)

// NewPubAckMessage creates a new PUBACK message.
func NewPubAckMessage() *PubAckMessage {
	msg := &PubAckMessage{}
	msg.SetType(PUBACK) // nolint: errcheck

	return msg
}

// String message as string
func (msg PubAckMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d", msg.header, msg.packetID)
}

// Len of message
func (msg *PubAckMessage) Len() int {
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
func (msg *PubAckMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := msg.header.decode(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	//this.packetID = binary.BigEndian.Uint16(src[total:])
	msg.packetID = src[total : total+2]
	total += 2

	msg.dirty = false

	return total, nil
}

// Encode message
func (msg *PubAckMessage) Encode(dst []byte) (int, error) {
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
	}

	return total, err
}

// Send encode and send message into ring buffer
func (msg *PubAckMessage) Send(to *buffer.Type) (int, error) {
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
			return total, err
		}
		total += n

		if copy(to.ExternalBuf[total:total+2], msg.packetID) != 2 {
			to.ExternalBuf[total] = 0
			to.ExternalBuf[total+1] = 0
		}

		total += 2

		total, err = to.Send(to.ExternalBuf[:total])
	}

	return total, err
}

func (msg *PubAckMessage) msgLen() int {
	// packet ID
	return 2
}
