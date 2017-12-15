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

import (
	"encoding/binary"
	"regexp"
	"unicode/utf8"
)

var clientIDRegexp *regexp.Regexp

func init() {
	// Added space for Paho compliance test
	// Added underscore (_) for MQTT C client test
	// Added coma(,) for MQTT C client test
	// Added slash(/) for GCP test
	clientIDRegexp = regexp.MustCompile(`^[0-9a-zA-Z \-_,.|/]*$`)
}

// Connect Accept After a Network Connection is established by a Client to a Server, the first Packet
// sent from the Client to the Server MUST be a CONNECT Packet [MQTT-3.1.0-1].
//
// A Client can only send the CONNECT Packet once over a Network Connection. The Server
// MUST process a second CONNECT Packet sent from a Client as a protocol violation and
// disconnect the Client [MQTT-3.1.0-2].  See section 4.8 for information about
// handling errors.
type Connect struct {
	header

	keepAlive uint16
	// 7: username flag
	// 6: password flag
	// 5: will retain
	// 4-3: will QoS
	// 2: will flag
	// 1: clean session
	// 0: reserved
	connectFlags byte
	clientID     []byte
	username     []byte
	password     []byte

	will struct {
		topic   string
		message []byte
	}
}

var _ Provider = (*Connect)(nil)

func newConnect() *Connect {
	return &Connect{}
}

// NewConnect creates a new CONNECT packet
func NewConnect(v ProtocolVersion) *Connect {
	p := newConnect()
	p.init(CONNECT, v, p.size, p.encodeMessage, p.decodeMessage)
	p.properties.reset()
	return p
}

// IsClean returns the bit that specifies the handling of the Session state.
// The Client and Server can store Session state to enable reliable messaging to
// continue across a sequence of Network Connections. This bit is used to control
// the lifetime of the Session state.
func (msg *Connect) IsClean() bool {
	return (msg.connectFlags & maskConnFlagClean) != 0
}

// SetClean sets the bit that specifies the handling of the Session state.
func (msg *Connect) SetClean(v bool) {
	if v {
		msg.connectFlags |= maskConnFlagClean // 0x02 // 00000010
	} else {
		msg.connectFlags &= ^maskConnFlagClean // 0xFD // 11111101
	}
}

// KeepAlive returns a time interval measured in seconds. Expressed as a 16-bit word,
// it is the maximum time interval that is permitted to elapse between the point at
// which the Client finishes transmitting one Control Packet and the point it starts
// sending the next.
func (msg *Connect) KeepAlive() uint16 {
	return msg.keepAlive
}

// SetKeepAlive sets the time interval in which the server should keep the connection
// alive.
func (msg *Connect) SetKeepAlive(v uint16) {
	msg.keepAlive = v
}

// ClientID returns an ID that identifies the Client to the Server. Each Client
// connecting to the Server has a unique ClientId. The ClientId MUST be used by
// Clients and by Servers to identify state that they hold relating to this MQTT
// Session between the Client and the Server
func (msg *Connect) ClientID() []byte {
	return msg.clientID
}

// SetClientID sets an ID that identifies the Client to the Server.
func (msg *Connect) SetClientID(v []byte) error {
	if !msg.validClientID(v) {
		return ErrInvalid
	}

	msg.clientID = v

	return nil
}

// Will returns the topic in which the Will Message should be published to.
// If the Will Flag is set to 1, the Will Topic must be in the payload.
// returns will topic, will message, will qos , will retain, will
// if last param is false will is not set
func (msg *Connect) Will() (string, []byte, QosType, bool, bool) {
	return msg.will.topic,
		msg.will.message,
		QosType((msg.connectFlags & maskConnFlagWillQos) >> offsetConnFlagWillQoS),
		(msg.connectFlags & maskConnFlagWillRetain) != 0,
		(msg.connectFlags & maskConnFlagWill) != 0
}

// SetWill state of message
func (msg *Connect) SetWill(t string, m []byte, qos QosType, retain bool) error {
	will := true
	if len(t) == 0 || len(m) == 0 || !qos.IsValid() {
		will = false
	}

	// Reset all will flags
	msg.ResetWill()

	if !will {
		return ErrInvalidArgs
	}

	if retain {
		msg.connectFlags |= maskConnFlagWillRetain
	}

	msg.connectFlags |= byte(qos) << offsetConnFlagWillQoS
	msg.connectFlags |= maskConnFlagWill
	msg.will.topic = t
	msg.will.message = m
	return nil
}

// ResetWill reset will state of message
func (msg *Connect) ResetWill() {
	msg.connectFlags &= ^maskConnFlagWill
	msg.connectFlags &= ^maskConnFlagWillQos
	msg.connectFlags &= ^maskConnFlagWillRetain
	msg.will.topic = ""
	msg.will.message = []byte{}
}

// Credentials returns user and password
func (msg *Connect) Credentials() ([]byte, []byte) {
	return msg.username, msg.password
}

// SetCredentials set username and password
func (msg *Connect) SetCredentials(u []byte, p []byte) error {
	msg.connectFlags &= ^maskConnFlagUsername
	msg.connectFlags &= ^maskConnFlagPassword

	// MQTT 3.1.1 does not allow password without user name
	if (len(msg.username) == 0 && len(msg.password) != 0) && msg.version < ProtocolV50 {
		return ErrInvalidArgs
	}

	if len(u) != 0 {
		if !utf8.Valid(u) {
			return ErrInvalidUtf8
		}
		msg.connectFlags |= maskConnFlagUsername
		msg.username = u
	}

	if len(p) != 0 {
		msg.connectFlags |= maskConnFlagPassword
		msg.password = p
	}

	return nil
}

// willFlag returns the bit that specifies whether a Will Message should be stored
// on the server. If the Will Flag is set to 1 this indicates that, if the Accept
// request is accepted, a Will Message MUST be stored on the Server and associated
// with the Network Connection.
func (msg *Connect) willFlag() bool {
	return (msg.connectFlags & maskConnFlagWill) != 0
}

// willQos returns the two bits that specify the QoS level to be used when publishing
// the Will Message.
func (msg *Connect) willQos() QosType {
	return QosType((msg.connectFlags & maskConnFlagWillQos) >> offsetConnFlagWillQoS)
}

// willRetain returns the bit specifies if the Will Message is to be Retained when it
// is published.
func (msg *Connect) willRetain() bool {
	return (msg.connectFlags & maskConnFlagWillRetain) != 0
}

// usernameFlag returns the bit that specifies whether a user name is present in the
// payload.
func (msg *Connect) usernameFlag() bool {
	return (msg.connectFlags & maskConnFlagUsername) != 0
}

// passwordFlag returns the bit that specifies whether a password is present in the
// payload.
func (msg *Connect) passwordFlag() bool {
	return (msg.connectFlags & maskConnFlagPassword) != 0
}

func (msg *Connect) encodeMessage(to []byte) (int, error) {
	if _, ok := SupportedVersions[msg.version]; !ok {
		return 0, ErrInvalidProtocolVersion
	}

	// V3.1.1 [MQTT-3.1.2.1]
	// V5.0   [MQTT-3.1.2.1]
	offset, err := WriteLPBytes(to, []byte(SupportedVersions[msg.version]))
	if err != nil {
		return offset, err
	}

	// V3.1.1 [MQTT-3.1.2.2]
	// V5.0   [MQTT-3.1.2.2]
	to[offset] = byte(msg.version)
	offset++

	// V3.1.1 [MQTT-3.1.2.3]
	// V5.0   [MQTT-3.1.2.3]
	to[offset] = msg.connectFlags
	offset++

	// V3.1.1 [MQTT-3.1.2.10]
	// V5.0   [MQTT-3.1.2.10]
	binary.BigEndian.PutUint16(to[offset:], msg.keepAlive)
	offset += 2

	var n int
	// V5.0   [MQTT-3.1.2.11]
	if msg.version >= ProtocolV50 {
		n, err = msg.properties.encode(to[offset:])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	// V3.1.1 [MQTT-3.1.3.1]
	// V5.0   [MQTT-3.1.3.1]
	n, err = WriteLPBytes(to[offset:], msg.clientID)
	offset += n
	if err != nil {
		return offset, err
	}

	if msg.willFlag() {
		// V3.1.1 [MQTT-3.1.3.2]
		// V5.0   [MQTT-3.1.3.2]
		n, err = WriteLPBytes(to[offset:], []byte(msg.will.topic))
		offset += n
		if err != nil {
			return offset, err
		}

		// V3.1.1 [MQTT-3.1.3.3]
		// V5.0   [MQTT-3.1.3.3]
		n, err = WriteLPBytes(to[offset:], msg.will.message)
		offset += n
		if err != nil {
			return offset, err
		}
	}

	if msg.usernameFlag() {
		// v3.1.1 [MQTT-3.1.3.4]
		// v5.0   [MQTT-3.1.3.4]
		n, err = WriteLPBytes(to[offset:], msg.username)
		offset += n
		if err != nil {
			return offset, err
		}
	}

	if msg.passwordFlag() {
		// v3.1.1 [MQTT-3.1.3.5]
		// v5.0   [MQTT-3.1.3.5]
		n, err = WriteLPBytes(to[offset:], msg.password)
		offset += n
		if err != nil {
			return offset, err
		}
	}

	return offset, nil
}

func (msg *Connect) decodeMessage(from []byte) (int, error) {
	var err error
	var n int
	offset := 0

	var protoName []byte

	// V3.1.1 [MQTT-3.1.2.1]
	// V5.0   [MQTT-3.1.2.1]
	if protoName, n, err = ReadLPBytes(from[offset:]); err != nil {
		return offset, err
	}
	offset += n

	// V3.1.1 [MQTT-3.1.2-1]
	// V5.0   [MQTT-3.1.2-1]
	if !utf8.Valid(protoName) {
		return offset, ErrProtocolInvalidName
	}

	// V3.1.1 [MQTT-3.1.2.2]
	// V5.0   [MQTT-3.1.2.2]
	msg.version = ProtocolVersion(from[offset])
	offset++

	// V3.1.1 [MQTT-3.1.2-2]
	// V5.0   [MQTT-3.1.2-2]
	if verStr, ok := SupportedVersions[msg.version]; !ok || verStr != string(protoName) {
		return offset, ErrInvalidProtocolVersion
	}

	// V3.1.1 [MQTT-3.1.2.3]
	// V5.0   [MQTT-3.1.2.3]
	msg.connectFlags = from[offset]
	offset++

	// V3.1.1 [MQTT-3.1.2-3]
	// V5.0   [MQTT-3.1.2-3]
	if msg.connectFlags&maskConnFlagReserved != 0 {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeMalformedPacket
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return offset, rejectCode
	}

	// V3.1.1 [MQTT-3.1.2-14]
	// V5.0   [MQTT-3.1.2-14]
	if !msg.willQos().IsValid() {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeMalformedPacket
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return offset, rejectCode
	}

	if !msg.willFlag() && (msg.willRetain() || (msg.willQos() != QoS0)) {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeMalformedPacket
		} else {
			rejectCode = CodeRefusedServerUnavailable
		}

		return offset, rejectCode
	}

	// V3.1.1 [MQTT-3.1.2-22].
	if (!msg.usernameFlag() && msg.passwordFlag()) && msg.version < ProtocolV50 {
		return offset, CodeRefusedBadUsernameOrPassword
	}

	// V3.1.1 [MQTT-3.1.2.10]
	// V5.0   [MQTT-3.1.2.10]
	msg.keepAlive = binary.BigEndian.Uint16(from[offset:])
	offset += 2

	// v5.0   [MQTT-3.1.2.11] specifies properties in variable header
	if msg.version >= ProtocolV50 {
		n, err = msg.properties.decode(msg.Type(), from[offset:])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	// V3.1.1 [MQTT-3.1.3.1]
	msg.clientID, n, err = ReadLPBytes(from[offset:])
	offset += n
	if err != nil {
		return offset, err
	}

	// V3.1.1  [MQTT-3.1.3-7]
	// If the Client supplies a zero-byte ClientId, the Client MUST also set CleanSession to 1
	if len(msg.clientID) == 0 && !msg.IsClean() {
		return offset, CodeRefusedIdentifierRejected
	}

	// The ClientId must contain only characters 0-9, a-z, and A-Z
	// We also support ClientId longer than 23 encoded bytes
	// We do not support ClientId outside of the above characters
	if !msg.validClientID(msg.clientID) {
		var rejectCode ReasonCode
		if msg.version == ProtocolV50 {
			rejectCode = CodeInvalidClientID
		} else {
			rejectCode = CodeRefusedIdentifierRejected
		}

		return offset, rejectCode
	}

	if msg.willFlag() {
		// V3.1.1 [MQTT-3.1.3.2]
		// V5.0   [MQTT-3.1.3.2]
		var buf []byte

		if buf, n, err = ReadLPBytes(from[offset:]); err != nil {
			return offset + n, err
		}
		offset += n

		msg.will.topic = string(buf)

		// V3.1.1 [3.1.3.3]
		// V5.0   [3.1.3.3]
		if buf, n, err = ReadLPBytes(from[offset:]); err != nil {
			return offset + n, err
		}
		offset += n

		msg.will.message = make([]byte, len(buf))
		copy(msg.will.message, buf)
	}

	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the user string is missing.

	// v3.1.1 [MQTT-3.1.3.4]
	// v5.0   [MQTT-3.1.3.4]
	if msg.usernameFlag() {
		if msg.username, n, err = ReadLPBytes(from[offset:]); err != nil {
			return offset + n, err
		}
		offset += n
	}

	// v3.1.1 [MQTT-3.1.3.5]
	// v5.0   [MQTT-3.1.3.5]
	if msg.passwordFlag() {
		if msg.password, n, err = ReadLPBytes(from[offset:]); err != nil {
			return offset + n, err
		}
		offset += n
	}

	return offset, nil
}

func (msg *Connect) size() int {
	total := 0

	version, ok := SupportedVersions[msg.version]
	if !ok {
		return total
	}

	//       2 bytes protocol name length
	//       |         n bytes protocol name
	//       |         |        1 byte protocol version
	//       |         |        |   1 byte connect flags
	//       |         |        |   |   2 bytes keep alive timer
	//       |         |        |   |   |
	total += 2 + len(version) + 1 + 1 + 2

	// v5.0 [MQTT-3.1.2.11]
	if msg.version >= ProtocolV50 {
		total += int(msg.properties.FullLen())
	}

	//       the length prefix
	//       |          length of client id
	//       |          |
	total += 2 + len(msg.clientID)

	// Add the will topic and will message length, and the length prefixes
	if msg.willFlag() {
		//       the length prefix of will topic
		//       |            length of will topic
		//       |            |            the length prefix of will message
		//       |            |            |            length of will message
		//       |            |            |            |
		total += 2 + len(msg.will.topic) + 2 + len(msg.will.message)
	}

	// Add the username length
	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the user name string is missing.
	if msg.usernameFlag() {
		total += 2 + len(msg.username)
	}

	// Add the password length
	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if msg.passwordFlag() {
		total += 2 + len(msg.password)
	}

	return total
}

// validClientID checks the client ID, which is a slice of bytes, to see if it's valid.
// Client ID is valid if it meets the requirement from the MQTT spec:
// 		The Server MUST allow ClientIds which are between 1 and 23 UTF-8 encoded bytes in length,
//		and that contain only the characters
//
//		"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
func (msg *Connect) validClientID(cid []byte) bool {
	// V3.1.1  [MQTT-3.1.3-6]
	// V5.0    [MQTT-3.1.3-6]
	if len(cid) == 0 {
		return true
	}

	// V3.1.1  [MQTT-3.1.3-4]      [MQTT-3.1.3-5]
	// V5.0    [MQTT-3.1.3-4]      [MQTT-3.1.3-5]
	return utf8.Valid(cid) && clientIDRegexp.Match(cid)
}
