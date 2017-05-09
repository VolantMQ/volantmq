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
)

// PubCompMessage The PUBCOMP Packet is the response to a PUBREL Packet. It is the fourth and
// final packet of the QoS 2 protocol exchange.
type PubCompMessage struct {
	header
}

var _ Provider = (*PubCompMessage)(nil)

// NewPubCompMessage creates a new PUBCOMP message.
func NewPubCompMessage() *PubCompMessage {
	msg := &PubCompMessage{}
	msg.SetType(PUBCOMP) // nolint: errcheck

	return msg
}

// String message as string
func (pam *PubCompMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d", pam.header, pam.packetID)
}

// Len of message
func (pam *PubCompMessage) Len() int {
	if !pam.dirty {
		return len(pam.dBuf)
	}

	ml := pam.msgLen()

	if err := pam.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return pam.header.msgLen() + ml
}

// Decode message
func (pam *PubCompMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := pam.header.decode(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	//this.packetID = binary.BigEndian.Uint16(src[total:])
	pam.packetID = src[total : total+2]
	total += 2

	pam.dirty = false

	return total, nil
}

// Encode message
func (pam *PubCompMessage) Encode(dst []byte) (int, error) {
	if !pam.dirty {
		if len(dst) < len(pam.dBuf) {
			return 0, ErrInsufficientBufferSize
		}

		return copy(dst, pam.dBuf), nil
	}

	hl := pam.header.msgLen()
	ml := pam.msgLen()

	if len(dst) < hl+ml {
		return 0, ErrInsufficientBufferSize
	}

	if err := pam.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := pam.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	if copy(dst[total:total+2], pam.packetID) != 2 {
		dst[total], dst[total+1] = 0, 0
	}
	total += 2

	return total, nil
}

func (pam *PubCompMessage) msgLen() int {
	// packet ID
	return 2
}
