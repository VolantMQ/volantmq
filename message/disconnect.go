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
	msg.setType(DISCONNECT) // nolint: errcheck
	msg.sizeCb = msg.size

	return msg
}

// decode message
func (msg *DisconnectMessage) decode(src []byte) (int, error) {
	return msg.header.decode(src)
}

func (msg *DisconnectMessage) preEncode(dst []byte) int {
	return msg.header.encode(dst)
}

// Encode message
func (msg *DisconnectMessage) Encode(dst []byte) (int, error) {
	expectedSize, err := msg.Size()
	if err != nil {
		return 0, err
	}

	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	return msg.preEncode(dst), nil
}

// Send encode and send message into ring buffer
func (msg *DisconnectMessage) Send(to *buffer.Type) (int, error) {
	expectedSize, err := msg.Size()
	if err != nil {
		return 0, err
	}

	if len(to.ExternalBuf) < expectedSize {
		to.ExternalBuf = make([]byte, expectedSize)
	}
	total := msg.preEncode(to.ExternalBuf)

	return to.Send([][]byte{to.ExternalBuf[:total]})
}

// Len of message
func (msg *DisconnectMessage) size() int {
	return 0
}
