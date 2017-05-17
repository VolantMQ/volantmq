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
	"github.com/troian/surgemq/buffer"
)

// PingRespMessage A PINGRESP Packet is sent by the Server to the Client in response to a PINGREQ
// Packet. It indicates that the Server is alive.
type PingRespMessage struct {
	header
}

var _ Provider = (*PingRespMessage)(nil)

// NewPingRespMessage creates a new PINGRESP message.
func NewPingRespMessage() Provider {
	msg := &PingRespMessage{}
	msg.SetType(PINGRESP) // nolint: errcheck

	return msg
}

// Decode message
func (msg *PingRespMessage) Decode(src []byte) (int, error) {
	return msg.header.decode(src)
}

func (msg *PingRespMessage) preEncode(dst []byte) (int, error) {
	var err error
	total := 0

	var n int

	if n, err = msg.header.encode(dst[total:]); err != nil {
		return total, err
	}
	total += n

	return total, err
}

// Encode message
func (msg *PingRespMessage) Encode(dst []byte) (int, error) {
	if len(dst) < msg.Len() {
		return 0, ErrInsufficientBufferSize
	}

	return msg.preEncode(dst)
}

// Send encode and send message into ring buffer
func (msg *PingRespMessage) Send(to *buffer.Type) (int, error) {
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
