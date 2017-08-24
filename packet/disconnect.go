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

// ReasonCode get disconnect reason
func (msg *Disconnect) ReasonCode() ReasonCode {
	return msg.reasonCode
}

// SetReasonCode set disconnect reason
func (msg *Disconnect) SetReasonCode(c ReasonCode) {
	msg.reasonCode = c
}

// decode message
func (msg *Disconnect) decodeMessage(src []byte) (int, error) {
	total := 0

	if msg.version == ProtocolV50 {
		if msg.remLen < 1 {
			return total, CodeMalformedPacket
		}

		msg.reasonCode = ReasonCode(src[total])
		if !msg.reasonCode.IsValidForType(msg.mType) {
			return total, CodeProtocolError
		}

		total++

		// V5.0 [MQTT-3.14.2.2.1]
		if len(src[total:]) < 1 && msg.remLen < 2 {
			return total, CodeMalformedPacket
		}

		if msg.remLen < 2 {
			total++
		} else {
			var err error
			var n int

			if msg.properties, n, err = decodeProperties(msg.mType, src[total:]); err != nil {
				return total + n, err
			}

			total += n
		}
	} else {
		if msg.remLen > 0 {
			return total, CodeRefusedServerUnavailable
		}
	}

	return total, nil
}

func (msg *Disconnect) encodeMessage(dst []byte) (int, error) {
	total := 0

	if msg.version == ProtocolV50 {
		dst[total] = byte(msg.reasonCode)
		total++

		var err error
		var n int

		if n, err = encodeProperties(msg.properties, dst[total:]); err != nil {
			return total + n, err
		}

		total += n
	}

	return total, nil
}

// Len of message
func (msg *Disconnect) size() int {
	total := 0

	if msg.version == ProtocolV50 {
		pLen, _ := encodeProperties(msg.properties, []byte{})
		total += 1 + pLen
	}

	return total
}
