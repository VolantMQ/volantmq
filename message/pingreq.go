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

// PingReqMessage The PINGREQ Packet is sent from a Client to the Server. It can be used to:
// 1. Indicate to the Server that the Client is alive in the absence of any other
//    Control Packets being sent from the Client to the Server.
// 2. Request that the Server responds to confirm that it is alive.
// 3. Exercise the network to indicate that the Network Connection is active.
type PingReqMessage struct {
	header
}

var _ Provider = (*PingReqMessage)(nil)

// NewPingReqMessage creates a new PINGREQ message.
func NewPingReqMessage() *PingReqMessage {
	msg := &PingReqMessage{}
	msg.SetType(PINGREQ) // nolint: errcheck

	return msg
}

// Decode message
func (msg *PingReqMessage) Decode(src []byte) (int, error) {
	return msg.header.decode(src)
}

// Encode message
func (msg *PingReqMessage) Encode(dst []byte) (int, error) {
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
func (msg *PingReqMessage) Send(to *buffer.Type) (int, error) {
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
		if n, err = msg.header.encode(to.ExternalBuf); err != nil {
			return total, err
		}
		total += n

		total, err = to.Send(to.ExternalBuf[:total])
	}

	return total, err
}
