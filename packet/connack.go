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

// ConnAck The CONNACK Packet is the packet sent by the Server in response to a CONNECT Packet
// received from a Client. The first packet sent from the Server to the Client MUST
// be a CONNACK Packet [MQTT-3.2.0-1].
// If the Client does not receive a CONNACK Packet from the Server within a reasonable
// amount of time, the Client SHOULD close the Network Connection. A "reasonable" amount
// of time depends on the type of application and the communications infrastructure.
type ConnAck struct {
	header

	sessionPresent bool
	returnCode     ReasonCode
}

var _ Provider = (*ConnAck)(nil)

// newConnAck creates a new CONNACK packet
func newConnAck() *ConnAck {
	return &ConnAck{}
}

// NewConnAck creates a new CONNACK packet
func NewConnAck(v ProtocolVersion) *ConnAck {
	p := newConnAck()

	p.init(CONNACK, v, p.size, p.encodeMessage, p.decodeMessage)

	return p
}

// SessionPresent returns the session present flag value
func (msg *ConnAck) SessionPresent() bool {
	return msg.sessionPresent
}

// SetSessionPresent sets the value of the session present flag
func (msg *ConnAck) SetSessionPresent(v bool) {
	msg.sessionPresent = v
}

// ReturnCode returns the return code received for the CONNECT message. The return
// type is an error
func (msg *ConnAck) ReturnCode() ReasonCode {
	return msg.returnCode
}

// SetReturnCode of conn
func (msg *ConnAck) SetReturnCode(ret ReasonCode) error {
	if !ret.IsValidForType(msg.Type()) {
		return ErrInvalidReturnCode
	}

	msg.returnCode = ret

	return nil
}

func (msg *ConnAck) decodeMessage(from []byte) (int, error) {
	offset := 0

	// [MQTT-3.2.2.1]
	b := from[offset]
	if b&(^maskConnAckSessionPresent) != 0 {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeMalformedPacket
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return offset, rejectCode
	}

	msg.sessionPresent = b&maskConnAckSessionPresent != 0
	offset++

	b = from[offset]
	msg.returnCode = ReasonCode(b)

	if !msg.returnCode.IsValidForType(msg.mType) {
		reason := CodeRefusedServerUnavailable
		if msg.version == ProtocolV50 {
			reason = CodeProtocolError
		}
		return offset, reason
	}

	offset++

	// v5 [MQTT-3.1.2.11] specifies properties in variable header
	if msg.version == ProtocolV50 {
		n, err := msg.properties.decode(msg.Type(), from[offset:])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	return offset, nil
}

func (msg *ConnAck) encodeMessage(to []byte) (int, error) {
	offset := 0

	if msg.sessionPresent {
		to[offset] = 1
	} else {
		to[offset] = 0
	}
	offset++

	to[offset] = msg.returnCode.Value()
	offset++

	var err error
	// V5.0   [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		var n int
		n, err = msg.properties.encode(to[offset:])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	return offset, err
}

func (msg *ConnAck) size() int {
	total := 2

	// v5.0 [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		total += int(msg.properties.FullLen())
	}

	return total
}
