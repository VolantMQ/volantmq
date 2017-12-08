// Copyright (c) 2014 The VolantMQ Authors. All rights reserved.
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

package packet

// Auth The CONNACK Packet is the packet sent by the Server in response to a CONNECT Packet
// received from a Client. The first packet sent from the Server to the Client MUST
// be a CONNACK Packet [MQTT-3.2.0-1].
// If the Client does not receive a CONNACK Packet from the Server within a reasonable
// amount of time, the Client SHOULD close the Network Connection. A "reasonable" amount
// of time depends on the type of application and the communications infrastructure.
type Auth struct {
	header

	authReason ReasonCode
}

var _ Provider = (*Auth)(nil)

// newAuth creates a new AUTH packet
func newAuth() *Auth {
	return &Auth{}
}

// NewAuth creates a new AUTH packet
func NewAuth(v ProtocolVersion) *Auth {
	p := newAuth()
	p.init(AUTH, v, p.size, p.encodeMessage, p.decodeMessage)

	return p
}

// ReasonCode get authentication reason
func (msg *Auth) ReasonCode() ReasonCode {
	return msg.authReason
}

// SetReasonCode set authentication reason code
func (msg *Auth) SetReasonCode(c ReasonCode) error {
	if msg.authReason.IsValidForType(msg.mType) {
		return ErrInvalidMessageType
	}

	msg.authReason = c

	return nil
}

// decode message
func (msg *Auth) decodeMessage(from []byte) (int, error) {
	offset := 0
	msg.authReason = ReasonCode(from[offset])

	if !msg.authReason.IsValidForType(msg.mType) {
		return offset, CodeProtocolError
	}

	n, err := msg.properties.decode(msg.Type(), from[offset:])
	return offset + n, err
}

func (msg *Auth) encodeMessage(to []byte) (int, error) {
	offset := 0
	to[offset] = byte(msg.authReason)
	n, err := msg.properties.encode(to[offset:])

	return offset + n, err
}

func (msg *Auth) size() int {
	return 1 + int(msg.properties.FullLen())
}
