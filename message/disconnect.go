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

// DisconnectMessage The DISCONNECT Packet is the final Control Packet sent from the Client to the Server.
// It indicates that the Client is disconnecting cleanly.
type DisconnectMessage struct {
	header
}

var _ Provider = (*DisconnectMessage)(nil)

// NewDisconnectMessage creates a new DISCONNECT message.
func NewDisconnectMessage() *DisconnectMessage {
	msg := &DisconnectMessage{}
	msg.SetType(DISCONNECT) // nolint: errcheck

	return msg
}

// Decode message
func (msg *DisconnectMessage) Decode(src []byte) (int, error) {
	return msg.header.decode(src)
}

// Encode message
func (msg *DisconnectMessage) Encode(dst []byte) (int, error) {
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
	}

	return total, err
}

// Send encode and send message into ring buffer
func (msg *DisconnectMessage) Send(to *buffer.Type) (int, error) {
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

		total, err = to.Send(to.ExternalBuf[:total])
	}

	return total, err
}
