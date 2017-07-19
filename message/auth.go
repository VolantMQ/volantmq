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

// AuthMessage The CONNACK Packet is the packet sent by the Server in response to a CONNECT Packet
// received from a Client. The first packet sent from the Server to the Client MUST
// be a CONNACK Packet [MQTT-3.2.0-1].
// If the Client does not receive a CONNACK Packet from the Server within a reasonable
// amount of time, the Client SHOULD close the Network Connection. A "reasonable" amount
// of time depends on the type of application and the communications infrastructure.
type AuthMessage struct {
	header

	authReason ReasonCode
}

var _ Provider = (*AuthMessage)(nil)

// NewConnAckMessage creates a new CONNACK message
func newAuthMessage() *AuthMessage {
	msg := &AuthMessage{}

	return msg
}

func (msg *AuthMessage) ReasonCode() ReasonCode {
	return msg.authReason
}

func (msg *AuthMessage) SetReasonCode(c ReasonCode) error {
	if msg.authReason.IsValidForType(msg.mType) {
		return ErrInvalidMessageType
	}

	msg.authReason = c

	return nil
}

// decode message
func (msg *AuthMessage) decodeMessage(src []byte) (int, error) {
	total := 0
	msg.authReason = ReasonCode(src[total])

	if !msg.authReason.IsValidForType(msg.mType) {
		return total, CodeProtocolError
	}

	var n int
	var err error
	msg.properties, n, err = decodeProperties(msg.Type(), src[total:])

	return total + n, err
}

func (msg *AuthMessage) encodeMessage(dst []byte) (int, error) {
	total := 0

	dst[total] = byte(msg.authReason)

	n, err := encodeProperties(msg.properties, dst[total:])

	return total + n, err
}

func (msg *AuthMessage) size() int {
	pLen, _ := encodeProperties(msg.properties, []byte{})
	return 1 + pLen
}
