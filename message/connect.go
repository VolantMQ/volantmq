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
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/troian/surgemq/buffer"
	"regexp"
)

var clientIDRegexp *regexp.Regexp

const (
	connFlagUsernameMask     byte = 0x80
	connFlagPasswordMask     byte = 0x40
	connFlagWillRetainMask   byte = 0x20
	connFlagWillQosMask      byte = 0x18
	connFlagWillMask         byte = 0x04
	connFlagCleanSessionMask byte = 0x02
)

func init() {
	// Added space for Paho compliance test
	// Added underscore (_) for MQTT C client test
	// Added coma(,) for MQTT C client test
	clientIDRegexp = regexp.MustCompile("^[0-9a-zA-Z _,]*$")
}

// ConnectMessage After a Network Connection is established by a Client to a Server, the first Packet
// sent from the Client to the Server MUST be a CONNECT Packet [MQTT-3.1.0-1].
//
// A Client can only send the CONNECT Packet once over a Network Connection. The Server
// MUST process a second CONNECT Packet sent from a Client as a protocol violation and
// disconnect the Client [MQTT-3.1.0-2].  See section 4.8 for information about
// handling errors.
type ConnectMessage struct {
	header

	// 7: username flag
	// 6: password flag
	// 5: will retain
	// 4-3: will QoS
	// 2: will flag
	// 1: clean session
	// 0: reserved
	connectFlags byte
	version      byte
	keepAlive    uint16
	protoName    []byte
	clientID     []byte
	willTopic    string
	willMessage  []byte
	username     []byte
	password     []byte
}

var _ Provider = (*ConnectMessage)(nil)

// NewConnectMessage creates a new CONNECT message.
func NewConnectMessage() *ConnectMessage {
	msg := &ConnectMessage{}
	msg.SetType(CONNECT) // nolint: errcheck

	return msg
}

// String returns a string representation of the CONNECT message
func (msg ConnectMessage) String() string {
	return fmt.Sprintf("%s, Connect Flags=%08b, Version=%d, KeepAlive=%d, Client ID=%q, Will Topic=%q, Will Message=%q, Username=%q, Password=%q",
		msg.header,
		msg.connectFlags,
		msg.Version(),
		msg.KeepAlive(),
		msg.ClientID(),
		msg.WillTopic(),
		msg.WillMessage(),
		msg.Username(),
		msg.Password(),
	)
}

// Version returns the the 8 bit unsigned value that represents the revision level
// of the protocol used by the Client. The value of the Protocol Level field for
// the version 3.1.1 of the protocol is 4 (0x04).
func (msg *ConnectMessage) Version() byte {
	return msg.version
}

// SetVersion sets the version value of the CONNECT message
func (msg *ConnectMessage) SetVersion(v byte) error {
	if _, ok := SupportedVersions[v]; !ok {
		return fmt.Errorf("connect/SetVersion: Invalid version number %d", v)
	}

	msg.version = v
	msg.dirty = true

	return nil
}

// CleanSession returns the bit that specifies the handling of the Session state.
// The Client and Server can store Session state to enable reliable messaging to
// continue across a sequence of Network Connections. This bit is used to control
// the lifetime of the Session state.
func (msg *ConnectMessage) CleanSession() bool {
	return (msg.connectFlags & connFlagCleanSessionMask) != 0
}

// SetCleanSession sets the bit that specifies the handling of the Session state.
func (msg *ConnectMessage) SetCleanSession(v bool) {
	if v {
		msg.connectFlags |= connFlagCleanSessionMask // 0x02 // 00000010
	} else {
		msg.connectFlags &= ^connFlagCleanSessionMask // 0xFD // 11111101
	}

	msg.dirty = true
}

// WillFlag returns the bit that specifies whether a Will Message should be stored
// on the server. If the Will Flag is set to 1 this indicates that, if the Connect
// request is accepted, a Will Message MUST be stored on the Server and associated
// with the Network Connection.
func (msg *ConnectMessage) WillFlag() bool {
	return (msg.connectFlags & connFlagWillMask) != 0
}

// SetWillFlag sets the bit that specifies whether a Will Message should be stored
// on the server.
func (msg *ConnectMessage) SetWillFlag(v bool) {
	if v {
		msg.connectFlags |= connFlagWillMask // 0x04 // 00000100
	} else {
		msg.connectFlags &= ^connFlagWillMask // 0xFB // 11111011
	}

	msg.dirty = true
}

// WillQos returns the two bits that specify the QoS level to be used when publishing
// the Will Message.
func (msg *ConnectMessage) WillQos() QosType {
	return QosType((msg.connectFlags & connFlagWillQosMask) >> 0x3)
}

// SetWillQos sets the two bits that specify the QoS level to be used when publishing
// the Will Message.
func (msg *ConnectMessage) SetWillQos(qos QosType) error {
	if !qos.IsValid() {
		return ErrInvalidQoS
	}

	msg.connectFlags &= ^connFlagWillQosMask
	msg.connectFlags |= byte(qos) << 0x03
	msg.dirty = true

	return nil
}

// WillRetain returns the bit specifies if the Will Message is to be Retained when it
// is published.
func (msg *ConnectMessage) WillRetain() bool {
	return (msg.connectFlags & connFlagWillRetainMask) != 0
}

// SetWillRetain sets the bit specifies if the Will Message is to be Retained when it
// is published.
func (msg *ConnectMessage) SetWillRetain(v bool) {
	if v {
		msg.connectFlags |= connFlagWillRetainMask // 0x20 // 00100000
	} else {
		msg.connectFlags &= ^connFlagWillRetainMask // 0xDF // 11011111
	}

	msg.dirty = true
}

// UsernameFlag returns the bit that specifies whether a user name is present in the
// payload.
func (msg *ConnectMessage) UsernameFlag() bool {
	return (msg.connectFlags & connFlagUsernameMask) != 0
}

// SetUsernameFlag sets the bit that specifies whether a user name is present in the
// payload.
func (msg *ConnectMessage) SetUsernameFlag(v bool) {
	if v {
		msg.connectFlags |= connFlagUsernameMask // 0x80 // 10000000
	} else {
		msg.connectFlags &= ^connFlagUsernameMask // 0x7F // 01111111
	}

	msg.dirty = true
}

// PasswordFlag returns the bit that specifies whether a password is present in the
// payload.
func (msg *ConnectMessage) PasswordFlag() bool {
	return (msg.connectFlags & connFlagPasswordMask) != 0
}

// SetPasswordFlag sets the bit that specifies whether a password is present in the
// payload.
func (msg *ConnectMessage) SetPasswordFlag(v bool) {
	if v {
		msg.connectFlags |= connFlagPasswordMask // 0x40 // 01000000
	} else {
		msg.connectFlags &= ^connFlagPasswordMask // 0xBF // 10111111
	}

	msg.dirty = true
}

// KeepAlive returns a time interval measured in seconds. Expressed as a 16-bit word,
// it is the maximum time interval that is permitted to elapse between the point at
// which the Client finishes transmitting one Control Packet and the point it starts
// sending the next.
func (msg *ConnectMessage) KeepAlive() uint16 {
	return msg.keepAlive
}

// SetKeepAlive sets the time interval in which the server should keep the connection
// alive.
func (msg *ConnectMessage) SetKeepAlive(v uint16) {
	msg.keepAlive = v

	msg.dirty = true
}

// ClientID returns an ID that identifies the Client to the Server. Each Client
// connecting to the Server has a unique ClientId. The ClientId MUST be used by
// Clients and by Servers to identify state that they hold relating to this MQTT
// Session between the Client and the Server
func (msg *ConnectMessage) ClientID() []byte {
	return msg.clientID
}

// SetClientID sets an ID that identifies the Client to the Server.
func (msg *ConnectMessage) SetClientID(v []byte) error {
	if len(v) > 0 && !msg.validClientID(v) {
		return ErrIdentifierRejected
	}

	msg.clientID = v
	msg.dirty = true

	return nil
}

// WillTopic returns the topic in which the Will Message should be published to.
// If the Will Flag is set to 1, the Will Topic must be in the payload.
func (msg *ConnectMessage) WillTopic() string {
	return msg.willTopic
}

// SetWillTopic sets the topic in which the Will Message should be published to.
func (msg *ConnectMessage) SetWillTopic(v string) {
	msg.willTopic = v

	if len(v) > 0 {
		msg.SetWillFlag(true)
	} else if len(msg.willMessage) == 0 {
		msg.SetWillFlag(false)
	}

	msg.dirty = true
}

// WillMessage returns the Will Message that is to be published to the Will Topic.
func (msg *ConnectMessage) WillMessage() []byte {
	return msg.willMessage
}

// SetWillMessage sets the Will Message that is to be published to the Will Topic.
func (msg *ConnectMessage) SetWillMessage(v []byte) {
	msg.willMessage = v

	if len(v) > 0 {
		msg.SetWillFlag(true)
	} else if len(msg.willTopic) == 0 {
		msg.SetWillFlag(false)
	}

	msg.dirty = true
}

// Username returns the username from the payload. If the User Name Flag is set to 1,
// this must be in the payload. It can be used by the Server for authentication and
// authorization.
func (msg *ConnectMessage) Username() []byte {
	return msg.username
}

// SetUsername sets the username for authentication.
func (msg *ConnectMessage) SetUsername(v []byte) {
	msg.username = v

	if len(v) > 0 {
		msg.SetUsernameFlag(true)
	} else {
		msg.SetUsernameFlag(false)
	}

	msg.dirty = true
}

// Password returns the password from the payload. If the Password Flag is set to 1,
// this must be in the payload. It can be used by the Server for authentication and
// authorization.
func (msg *ConnectMessage) Password() []byte {
	return msg.password
}

// SetPassword sets the username for authentication.
func (msg *ConnectMessage) SetPassword(v []byte) {
	msg.password = v

	if len(v) > 0 {
		msg.SetPasswordFlag(true)
	} else {
		msg.SetPasswordFlag(false)
	}

	msg.dirty = true
}

// Len of message
func (msg *ConnectMessage) Len() int {
	if !msg.dirty {
		return len(msg.dBuf)
	}

	ml := msg.msgLen()

	if err := msg.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return msg.header.msgLen() + ml
}

// Decode For the CONNECT message, the error returned could be a ConnAckReturnCode, so
// be sure to check that. Otherwise it's a generic error. If a generic error is
// returned, this Message should be considered invalid.
//
// Caller should call ValidConnAckError(err) to see if the returned error is
// a ConnAck error. If so, caller should send the Client back the corresponding
// CONNACK message.
func (msg *ConnectMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := msg.header.decode(src[total:])
	if err != nil {
		return total + n, err
	}
	total += n

	if n, err = msg.decodeMessage(src[total:]); err != nil {
		return total + n, err
	}
	total += n

	msg.dirty = false

	return total, nil
}

// Encode message
func (msg *ConnectMessage) Encode(dst []byte) (int, error) {
	expectedSize := msg.Len()
	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	var err error
	total := 0

	if !msg.dirty {
		total = copy(dst, msg.dBuf)
	} else {
		if msg.Type() != CONNECT {
			return 0, ErrInvalidMessageType
		}

		if _, ok := SupportedVersions[msg.version]; !ok {
			return 0, ErrInvalidProtocolVersion
		}

		if err = msg.SetRemainingLength(int32(msg.msgLen())); err != nil {
			return 0, err
		}

		var n int

		if n, err = msg.header.encode(dst[total:]); err != nil {
			return total, err
		}
		total += n

		if n, err = msg.encodeMessage(dst[total:]); err != nil {
			return total, err
		}
		total += n
	}

	return total, err
}

// Send encode and send message into ring buffer
func (msg *ConnectMessage) Send(to *buffer.Type) (int, error) {
	var err error
	total := 0

	if !msg.dirty {
		total, err = to.Send(msg.dBuf)
	} else {
		if msg.Type() != CONNECT {
			return 0, ErrInvalidMessageType
		}

		if _, ok := SupportedVersions[msg.version]; !ok {
			return 0, ErrInvalidProtocolVersion
		}

		expectedSize := msg.Len()
		if len(to.ExternalBuf) < expectedSize {
			to.ExternalBuf = make([]byte, expectedSize)
		}

		var n int

		if n, err = msg.header.encode(to.ExternalBuf[total:]); err != nil {
			return total, err
		}
		total += n

		if n, err = msg.encodeMessage(to.ExternalBuf[total:]); err != nil {
			return total, err
		}
		total += n

		total, err = to.Send(to.ExternalBuf[:total])
	}

	return total, err
}

func (msg *ConnectMessage) encodeMessage(dst []byte) (int, error) {
	total := 0

	n, err := writeLPBytes(dst[total:], []byte(SupportedVersions[msg.version]))
	total += n
	if err != nil {
		return total, err
	}

	dst[total] = msg.version
	total++

	dst[total] = msg.connectFlags
	total++

	binary.BigEndian.PutUint16(dst[total:], msg.keepAlive)
	total += 2

	n, err = writeLPBytes(dst[total:], msg.clientID)
	total += n
	if err != nil {
		return total, err
	}

	if msg.WillFlag() {
		n, err = writeLPBytes(dst[total:], []byte(msg.willTopic))
		total += n
		if err != nil {
			return total, err
		}

		n, err = writeLPBytes(dst[total:], msg.willMessage)
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the username string is missing.
	if msg.UsernameFlag() && len(msg.username) > 0 {
		n, err = writeLPBytes(dst[total:], msg.username)
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if msg.PasswordFlag() && len(msg.password) > 0 {
		n, err = writeLPBytes(dst[total:], msg.password)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (msg *ConnectMessage) decodeMessage(src []byte) (int, error) {
	var err error
	var n int
	total := 0

	msg.protoName, n, err = readLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	msg.version = src[total]
	total++

	if verstr, ok := SupportedVersions[msg.version]; !ok {
		return total, ErrInvalidProtocolVersion
	} else if verstr != string(msg.protoName) {
		return total, ErrInvalidProtocolVersion
	}

	msg.connectFlags = src[total]
	total++

	if msg.connectFlags&0x1 != 0 {
		return total, errors.New("connect/decodeMessage: Connect Flags reserved bit 0 is not 0")
	}

	if !msg.WillQos().IsValid() {
		return total, ErrInvalidQoS
	}

	if !msg.WillFlag() && (msg.WillRetain() || (msg.WillQos() != QosAtMostOnce)) {
		return total, ErrProtocolViolation
	}

	if !msg.UsernameFlag() && msg.PasswordFlag() {
		return total, ErrBadUsernameOrPassword //errors.New("connect/decodeMessage: Username flag is set but Password flag is not set")
	}

	if len(src[total:]) < 2 {
		return 0, ErrInsufficientBufferSize
	}

	msg.keepAlive = binary.BigEndian.Uint16(src[total:])
	total += 2

	msg.clientID, n, err = readLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	// If the Client supplies a zero-byte ClientId, the Client MUST also set CleanSession to 1
	if len(msg.clientID) == 0 && !msg.CleanSession() {
		return total, ErrIdentifierRejected
	}

	// The ClientId must contain only characters 0-9, a-z, and A-Z
	// We also support ClientId longer than 23 encoded bytes
	// We do not support ClientId outside of the above characters
	if len(msg.clientID) > 0 && !msg.validClientID(msg.clientID) {
		return total, ErrIdentifierRejected
	}

	if msg.WillFlag() {
		var willBuf []byte
		willBuf, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		msg.willTopic = string(willBuf)

		msg.willMessage, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the user string is missing.
	if msg.UsernameFlag() && len(src[total:]) > 0 {
		msg.username, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if msg.PasswordFlag() && len(src[total:]) > 0 {
		msg.password, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (msg *ConnectMessage) msgLen() int {
	total := 0

	verstr, ok := SupportedVersions[msg.version]
	if !ok {
		return total
	}

	// 2 bytes protocol name length
	// n bytes protocol name
	// 1 byte protocol version
	// 1 byte connect flags
	// 2 bytes keep alive timer
	total += 2 + len(verstr) + 1 + 1 + 2

	// Add the clientID length, 2 is the length prefix
	total += 2 + len(msg.clientID)

	// Add the will topic and will message length, and the length prefixes
	if msg.WillFlag() {
		total += 2 + len(msg.willTopic) + 2 + len(msg.willMessage)
	}

	// Add the username length
	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the user name string is missing.
	if msg.UsernameFlag() && len(msg.username) > 0 {
		total += 2 + len(msg.username)
	}

	// Add the password length
	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if msg.PasswordFlag() && len(msg.password) > 0 {
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
func (msg *ConnectMessage) validClientID(cid []byte) bool {
	// Fixed https://github.com/surgemq/surgemq/issues/4
	//if len(cid) > 23 {
	//	return false
	//}

	if msg.Version() == 0x3 {
		return true
	}

	return clientIDRegexp.Match(cid)
}
