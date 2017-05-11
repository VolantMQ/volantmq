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
	"errors"
	"fmt"
	"github.com/troian/surgemq/buffer"
)

// ConnAckMessage The CONNACK Packet is the packet sent by the Server in response to a CONNECT Packet
// received from a Client. The first packet sent from the Server to the Client MUST
// be a CONNACK Packet [MQTT-3.2.0-1].
// If the Client does not receive a CONNACK Packet from the Server within a reasonable
// amount of time, the Client SHOULD close the Network Connection. A "reasonable" amount
// of time depends on the type of application and the communications infrastructure.
type ConnAckMessage struct {
	header

	sessionPresent bool
	returnCode     ConnAckCode
}

var _ Provider = (*ConnAckMessage)(nil)

// NewConnAckMessage creates a new CONNACK message
func NewConnAckMessage() *ConnAckMessage {
	msg := &ConnAckMessage{}
	msg.SetType(CONNACK) // nolint: errcheck

	return msg
}

// String returns a string representation of the CONNACK message
func (msg ConnAckMessage) String() string {
	return fmt.Sprintf("%s, Session Present=%t, Return code=%q\n", msg.header, msg.sessionPresent, msg.returnCode)
}

// SessionPresent returns the session present flag value
func (msg *ConnAckMessage) SessionPresent() bool {
	return msg.sessionPresent
}

// SetSessionPresent sets the value of the session present flag
func (msg *ConnAckMessage) SetSessionPresent(v bool) {
	msg.sessionPresent = v
	msg.dirty = true
}

// ReturnCode returns the return code received for the CONNECT message. The return
// type is an error
func (msg *ConnAckMessage) ReturnCode() ConnAckCode {
	return msg.returnCode
}

// SetReturnCode of conn
func (msg *ConnAckMessage) SetReturnCode(ret ConnAckCode) {
	msg.returnCode = ret
	msg.dirty = true
}

// Len of message
func (msg *ConnAckMessage) Len() int {
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
func (msg *ConnAckMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := msg.header.decode(src)
	total += n
	if err != nil {
		return total, err
	}

	b := src[total]

	if b&254 != 0 {
		return 0, errors.New("connack/Decode: Bits 7-1 in Connack Acknowledge Flags byte (1) are not 0")
	}

	msg.sessionPresent = b&0x01 != 0
	total++

	b = src[total]

	// Read return code
	if b > 5 {
		return 0, ErrInvalidReturnCode
	}

	msg.returnCode = ConnAckCode(b)
	total++

	msg.dirty = false

	return total, nil
}

//Encode message
func (msg *ConnAckMessage) Encode(dst []byte) (int, error) {
	expectedSize := msg.Len()
	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	var err error
	total := 0

	if !msg.dirty {
		total = copy(dst, msg.dBuf)
	} else {
		if msg.returnCode > 5 {
			return total, ErrInvalidReturnCode
		}

		if err = msg.SetRemainingLength(int32(msg.msgLen())); err != nil {
			return 0, err
		}

		var n int

		if n, err = msg.header.encode(dst[total:]); err != nil {
			return 0, err
		}
		total += n

		if msg.sessionPresent {
			dst[total] = 1
		} else {
			dst[total] = 0
		}
		total++

		dst[total] = msg.returnCode.Value()
		total++
	}

	return total, err
}

// Send encode and send message into ring buffer
func (msg *ConnAckMessage) Send(to *buffer.Type) (int, error) {
	var err error
	total := 0

	if !msg.dirty {
		total, err = to.Send(msg.dBuf)
	} else {
		if msg.returnCode > 5 {
			return total, ErrInvalidReturnCode
		}

		expectedSize := msg.Len()
		if len(to.ExternalBuf) < expectedSize {
			to.ExternalBuf = make([]byte, expectedSize)
		}

		var n int

		if n, err = msg.header.encode(to.ExternalBuf[total:]); err != nil {
			return 0, err
		}
		total += n

		if msg.sessionPresent {
			to.ExternalBuf[total] = 1
		} else {
			to.ExternalBuf[total] = 0
		}
		total++

		to.ExternalBuf[total] = msg.returnCode.Value()
		total++

		total, err = to.Send(to.ExternalBuf[:total])
	}

	return total, err
}

func (msg *ConnAckMessage) msgLen() int {
	return 2
}
