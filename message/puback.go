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

// A PUBACK Packet is the response to a PUBLISH Packet with QoS level 1.
type PubAckMessage struct {
	header
}

var _ Message = (*PubAckMessage)(nil)

// NewPubAckMessage creates a new PUBACK message.
func NewPubAckMessage() *PubAckMessage {
	msg := &PubAckMessage{}
	msg.SetType(PUBACK)

	return msg
}

func (pam PubAckMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d", pam.header, pam.packetId)
}

func (pam *PubAckMessage) Len() int {
	if !pam.dirty {
		return len(pam.dbuf)
	}

	ml := pam.msglen()

	if err := pam.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return pam.header.msglen() + ml
}

func (pam *PubAckMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := pam.header.decode(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	//this.packetId = binary.BigEndian.Uint16(src[total:])
	pam.packetId = src[total : total+2]
	total += 2

	pam.dirty = false

	return total, nil
}

func (pam *PubAckMessage) Encode(dst []byte) (int, error) {
	if !pam.dirty {
		if len(dst) < len(pam.dbuf) {
			return 0, fmt.Errorf("puback/Encode: Insufficient buffer size. Expecting %d, got %d.", len(pam.dbuf), len(dst))
		}

		return copy(dst, pam.dbuf), nil
	}

	hl := pam.header.msglen()
	ml := pam.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("puback/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
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

	if copy(dst[total:total+2], pam.packetId) != 2 {
		dst[total], dst[total+1] = 0, 0
	}
	total += 2

	return total, nil
}

func (pam *PubAckMessage) msglen() int {
	// packet ID
	return 2
}
