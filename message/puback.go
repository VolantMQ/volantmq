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

// SetPacketID sets the ID of the packet.
func (msg *PubAckMessage) SetPacketID(v uint16) {
	msg.packetID = v
}

// Len of message
func (msg *PubAckMessage) Len() int {
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

	msg.packetID = binary.BigEndian.Uint16(src[total:])
	total += 2

	return total, nil
}

func (msg *PubAckMessage) preEncode(dst []byte) (int, error) {
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

	return total, err
}

// Encode message
func (msg *PubAckMessage) Encode(dst []byte) (int, error) {
	expectedSize := msg.Len()
	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	return msg.preEncode(dst)
}

// Send encode and send message into ring buffer
func (msg *PubAckMessage) Send(to *buffer.Type) (int, error) {
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

func (msg *PubAckMessage) msgLen() int {
	// packet ID
	return 2
}
