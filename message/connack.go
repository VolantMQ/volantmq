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

import "errors"

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
	msg.setType(CONNACK) // nolint: errcheck
	msg.sizeCb = msg.size

	return msg
}

// SessionPresent returns the session present flag value
func (msg *ConnAckMessage) SessionPresent() bool {
	return msg.sessionPresent
}

// SetSessionPresent sets the value of the session present flag
func (msg *ConnAckMessage) SetSessionPresent(v bool) {
	msg.sessionPresent = v
}

// ReturnCode returns the return code received for the CONNECT message. The return
// type is an error
func (msg *ConnAckMessage) ReturnCode() ConnAckCode {
	return msg.returnCode
}

// SetReturnCode of conn
func (msg *ConnAckMessage) SetReturnCode(ret ConnAckCode) error {
	if !ret.Valid() {
		return ErrInvalidReturnCode
	}

	msg.returnCode = ret

	return nil
}

// decode message
func (msg *ConnAckMessage) decode(src []byte) (int, error) {
	total := 0

	n, err := msg.header.decode(src)
	total += n
	if err != nil {
		return total, err
	}

	// [MQTT-3.2.2.1]
	b := src[total]
	if b&(^maskConnAckSessionPresent) != 0 {
		return 0, errors.New("connack/decode: Bits 7-1 in Connack Acknowledge Flags byte (1) are not 0")
	}

	msg.sessionPresent = b&maskConnAckSessionPresent != 0
	total++

	b = src[total]
	// [MQTT-3.2.2.3] Read return code
	msg.returnCode = ConnAckCode(b)
	if msg.returnCode >= ConnAckCodeReserved {
		return 0, ErrInvalidReturnCode
	}
	total++

	return total, nil
}

func (msg *ConnAckMessage) preEncode(dst []byte) int {
	total := 0

	total += msg.header.encode(dst[total:])

	if msg.sessionPresent {
		dst[total] = 1
	} else {
		dst[total] = 0
	}
	total++

	dst[total] = msg.returnCode.Value()
	total++

	return total
}

//Encode message
func (msg *ConnAckMessage) Encode(dst []byte) (int, error) {
	expectedSize, _ := msg.Size()
	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	return msg.preEncode(dst), nil
}

func (msg *ConnAckMessage) size() int {
	return 2
}
