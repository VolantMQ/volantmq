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

// Disconnect The DISCONNECT Packet is the final Control Packet sent from the Client to the Server.
// It indicates that the Client is disconnecting cleanly.
type Disconnect struct {
	header

	reasonCode ReasonCode
}

var _ Provider = (*Disconnect)(nil)

func newDisconnect() *Disconnect {
	return &Disconnect{}
}

// NewDisconnect creates a new DISCONNECT packet
func NewDisconnect(v ProtocolVersion) *Disconnect {
	p := newDisconnect()
	p.init(DISCONNECT, v, p.size, p.encodeMessage, p.decodeMessage)
	return p
}

// ReasonCode get disconnect reason
func (msg *Disconnect) ReasonCode() ReasonCode {
	return msg.reasonCode
}

// SetReasonCode set disconnect reason
func (msg *Disconnect) SetReasonCode(c ReasonCode) {
	msg.reasonCode = c
}

// decode message
func (msg *Disconnect) decodeMessage(from []byte) (int, error) {
	offset := 0

	if msg.version == ProtocolV50 {
		// [MQTT-3.14.2.1]
		if msg.remLen < 1 {
			msg.reasonCode = CodeSuccess
			return offset, nil
		}

		msg.reasonCode = ReasonCode(from[offset])
		if !msg.reasonCode.IsValidForType(msg.mType) {
			return offset, CodeProtocolError
		}

		offset++

		// V5.0 [MQTT-3.14.2.2.1]
		if len(from[offset:]) < 1 && msg.remLen < 2 {
			return offset, CodeMalformedPacket
		}

		if msg.remLen < 2 {
			offset++
		} else {
			n, err := msg.properties.decode(msg.Type(), from[offset:])
			offset += n
			if err != nil {
				return offset, err
			}
		}
	} else {
		if msg.remLen > 0 {
			return offset, CodeRefusedServerUnavailable
		}
	}

	return offset, nil
}

func (msg *Disconnect) encodeMessage(to []byte) (int, error) {
	offset := 0

	var err error
	if msg.version == ProtocolV50 {
		pLen := msg.properties.FullLen()
		// The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Normal disconnection)
		// and there are no Properties. In this case the DISCONNECT has a Remaining Length of 0
		if pLen > 1 || msg.reasonCode != CodeSuccess {
			to[offset] = byte(msg.reasonCode)
			offset++

			// [MQTT-3.14.2.2.1]
			if pLen > 1 {
				var n int
				n, err = msg.properties.encode(to[offset:])
				offset += n
			}
		}
	}

	return offset, err
}

// Len of message
func (msg *Disconnect) size() int {
	total := 0

	if msg.version == ProtocolV50 {
		pLen := msg.properties.FullLen()
		// If properties exist (which indicated when pLen > 1) include in body size reason code and properties
		// otherwise include only reason code if it differs from CodeSuccess
		if pLen > 1 || msg.reasonCode != CodeSuccess {
			total++
			if pLen > 1 {
				total += int(pLen)
			}
		}
	}

	return total
}
