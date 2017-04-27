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
	"errors"
)

// The CONNACK Packet is the packet sent by the Server in response to a CONNECT Packet
// received from a Client. The first packet sent from the Server to the Client MUST
// be a CONNACK Packet [MQTT-3.2.0-1].
//
// If the Client does not receive a CONNACK Packet from the Server within a reasonable
// amount of time, the Client SHOULD close the Network Connection. A "reasonable" amount
// of time depends on the type of application and the communications infrastructure.
type ConnAckMessage struct {
	header

	sessionPresent bool
	returnCode     ConnAckCode
}

var _ Message = (*ConnAckMessage)(nil)

// NewConnAckMessage creates a new CONNACK message
func NewConnAckMessage() *ConnAckMessage {
	msg := &ConnAckMessage{}
	msg.SetType(CONNACK)

	return msg
}

// String returns a string representation of the CONNACK message
func (cm ConnAckMessage) String() string {
	return fmt.Sprintf("%s, Session Present=%t, Return code=%q\n", cm.header, cm.sessionPresent, cm.returnCode)
}

// SessionPresent returns the session present flag value
func (cm *ConnAckMessage) SessionPresent() bool {
	return cm.sessionPresent
}

// SetSessionPresent sets the value of the session present flag
func (cm *ConnAckMessage) SetSessionPresent(v bool) {
	if v {
		cm.sessionPresent = true
	} else {
		cm.sessionPresent = false
	}

	cm.dirty = true
}

// ReturnCode returns the return code received for the CONNECT message. The return
// type is an error
func (cm *ConnAckMessage) ReturnCode() ConnAckCode {
	return cm.returnCode
}

func (cm *ConnAckMessage) SetReturnCode(ret ConnAckCode) {
	cm.returnCode = ret
	cm.dirty = true
}

func (cm *ConnAckMessage) Len() int {
	if !cm.dirty {
		return len(cm.dBuf)
	}

	ml := cm.msgLen()

	if err := cm.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return cm.header.msgLen() + ml
}

func (cm *ConnAckMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := cm.header.decode(src)
	total += n
	if err != nil {
		return total, err
	}

	b := src[total]

	if b&254 != 0 {
		return 0, errors.New("connack/Decode: Bits 7-1 in Connack Acknowledge Flags byte (1) are not 0")
	}

	cm.sessionPresent = b&0x1 == 1
	total++

	b = src[total]

	// Read return code
	if b > 5 {
		return 0, fmt.Errorf("connack/Decode: Invalid CONNACK return code (%d)", b)
	}

	cm.returnCode = ConnAckCode(b)
	total++

	cm.dirty = false

	return total, nil
}

func (cm *ConnAckMessage) Encode(dst []byte) (int, error) {
	if !cm.dirty {
		if len(dst) < len(cm.dBuf) {
			return 0, fmt.Errorf("connack/Encode: Insufficient buffer size. Expecting %d, got %d.", len(cm.dBuf), len(dst))
		}

		return copy(dst, cm.dBuf), nil
	}

	// CONNACK remaining length fixed at 2 bytes
	hl := cm.header.msgLen()
	ml := cm.msgLen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("connack/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := cm.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := cm.header.encode(dst[total:])
	total += n
	if err != nil {
		return 0, err
	}

	if cm.sessionPresent {
		dst[total] = 1
	}
	total++

	if cm.returnCode > 5 {
		return total, fmt.Errorf("connack/Encode: Invalid CONNACK return code (%d)", cm.returnCode)
	}

	dst[total] = cm.returnCode.Value()
	total++

	return total, nil
}

func (cm *ConnAckMessage) msgLen() int {
	return 2
}
