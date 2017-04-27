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
	"fmt"
	"github.com/pkg/errors"
	"regexp"
)

var clientIdRegexp *regexp.Regexp

func init() {
	// Added space for Paho compliance test
	// Added underscore (_) for MQTT C client test
	clientIdRegexp = regexp.MustCompile("^[0-9a-zA-Z _]*$")
}

// After a Network Connection is established by a Client to a Server, the first Packet
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

	version byte

	keepAlive uint16

	protoName,
	clientId,
	willTopic,
	willMessage,
	username,
	password []byte
}

var _ Message = (*ConnectMessage)(nil)

// NewConnectMessage creates a new CONNECT message.
func NewConnectMessage() *ConnectMessage {
	msg := &ConnectMessage{}
	msg.SetType(CONNECT)

	return msg
}

// String returns a string representation of the CONNECT message
func (cm ConnectMessage) String() string {
	return fmt.Sprintf("%s, Connect Flags=%08b, Version=%d, KeepAlive=%d, Client ID=%q, Will Topic=%q, Will Message=%q, Username=%q, Password=%q",
		cm.header,
		cm.connectFlags,
		cm.Version(),
		cm.KeepAlive(),
		cm.ClientId(),
		cm.WillTopic(),
		cm.WillMessage(),
		cm.Username(),
		cm.Password(),
	)
}

// Version returns the the 8 bit unsigned value that represents the revision level
// of the protocol used by the Client. The value of the Protocol Level field for
// the version 3.1.1 of the protocol is 4 (0x04).
func (cm *ConnectMessage) Version() byte {
	return cm.version
}

// SetVersion sets the version value of the CONNECT message
func (cm *ConnectMessage) SetVersion(v byte) error {
	if _, ok := SupportedVersions[v]; !ok {
		return fmt.Errorf("connect/SetVersion: Invalid version number %d", v)
	}

	cm.version = v
	cm.dirty = true

	return nil
}

// CleanSession returns the bit that specifies the handling of the Session state.
// The Client and Server can store Session state to enable reliable messaging to
// continue across a sequence of Network Connections. This bit is used to control
// the lifetime of the Session state.
func (cm *ConnectMessage) CleanSession() bool {
	return ((cm.connectFlags >> 1) & 0x1) == 1
}

// SetCleanSession sets the bit that specifies the handling of the Session state.
func (cm *ConnectMessage) SetCleanSession(v bool) {
	if v {
		cm.connectFlags |= 0x2 // 00000010
	} else {
		cm.connectFlags &= 253 // 11111101
	}

	cm.dirty = true
}

// WillFlag returns the bit that specifies whether a Will Message should be stored
// on the server. If the Will Flag is set to 1 this indicates that, if the Connect
// request is accepted, a Will Message MUST be stored on the Server and associated
// with the Network Connection.
func (cm *ConnectMessage) WillFlag() bool {
	return ((cm.connectFlags >> 2) & 0x1) == 1
}

// SetWillFlag sets the bit that specifies whether a Will Message should be stored
// on the server.
func (cm *ConnectMessage) SetWillFlag(v bool) {
	if v {
		cm.connectFlags |= 0x4 // 00000100
	} else {
		cm.connectFlags &= 251 // 11111011
	}

	cm.dirty = true
}

// WillQos returns the two bits that specify the QoS level to be used when publishing
// the Will Message.
func (cm *ConnectMessage) WillQos() byte {
	return (cm.connectFlags >> 3) & 0x3
}

// SetWillQos sets the two bits that specify the QoS level to be used when publishing
// the Will Message.
func (cm *ConnectMessage) SetWillQos(qos byte) error {
	if qos != QosAtMostOnce && qos != QosAtLeastOnce && qos != QosExactlyOnce {
		return fmt.Errorf("connect/SetWillQos: Invalid QoS level %d", qos)
	}

	cm.connectFlags = (cm.connectFlags & 231) | (qos << 3) // 231 = 11100111
	cm.dirty = true

	return nil
}

// WillRetain returns the bit specifies if the Will Message is to be Retained when it
// is published.
func (cm *ConnectMessage) WillRetain() bool {
	return ((cm.connectFlags >> 5) & 0x1) == 1
}

// SetWillRetain sets the bit specifies if the Will Message is to be Retained when it
// is published.
func (cm *ConnectMessage) SetWillRetain(v bool) {
	if v {
		cm.connectFlags |= 32 // 00100000
	} else {
		cm.connectFlags &= 223 // 11011111
	}

	cm.dirty = true
}

// UsernameFlag returns the bit that specifies whether a user name is present in the
// payload.
func (cm *ConnectMessage) UsernameFlag() bool {
	return ((cm.connectFlags >> 7) & 0x1) == 1
}

// SetUsernameFlag sets the bit that specifies whether a user name is present in the
// payload.
func (cm *ConnectMessage) SetUsernameFlag(v bool) {
	if v {
		cm.connectFlags |= 128 // 10000000
	} else {
		cm.connectFlags &= 127 // 01111111
	}

	cm.dirty = true
}

// PasswordFlag returns the bit that specifies whether a password is present in the
// payload.
func (cm *ConnectMessage) PasswordFlag() bool {
	return ((cm.connectFlags >> 6) & 0x1) == 1
}

// SetPasswordFlag sets the bit that specifies whether a password is present in the
// payload.
func (cm *ConnectMessage) SetPasswordFlag(v bool) {
	if v {
		cm.connectFlags |= 64 // 01000000
	} else {
		cm.connectFlags &= 191 // 10111111
	}

	cm.dirty = true
}

// KeepAlive returns a time interval measured in seconds. Expressed as a 16-bit word,
// it is the maximum time interval that is permitted to elapse between the point at
// which the Client finishes transmitting one Control Packet and the point it starts
// sending the next.
func (cm *ConnectMessage) KeepAlive() uint16 {
	return cm.keepAlive
}

// SetKeepAlive sets the time interval in which the server should keep the connection
// alive.
func (cm *ConnectMessage) SetKeepAlive(v uint16) {
	cm.keepAlive = v

	cm.dirty = true
}

// ClientId returns an ID that identifies the Client to the Server. Each Client
// connecting to the Server has a unique ClientId. The ClientId MUST be used by
// Clients and by Servers to identify state that they hold relating to this MQTT
// Session between the Client and the Server
func (cm *ConnectMessage) ClientId() []byte {
	return cm.clientId
}

// SetClientId sets an ID that identifies the Client to the Server.
func (cm *ConnectMessage) SetClientId(v []byte) error {
	if len(v) > 0 && !cm.validClientId(v) {
		return ErrIdentifierRejected
	}

	cm.clientId = v
	cm.dirty = true

	return nil
}

// WillTopic returns the topic in which the Will Message should be published to.
// If the Will Flag is set to 1, the Will Topic must be in the payload.
func (cm *ConnectMessage) WillTopic() []byte {
	return cm.willTopic
}

// SetWillTopic sets the topic in which the Will Message should be published to.
func (cm *ConnectMessage) SetWillTopic(v []byte) {
	cm.willTopic = v

	if len(v) > 0 {
		cm.SetWillFlag(true)
	} else if len(cm.willMessage) == 0 {
		cm.SetWillFlag(false)
	}

	cm.dirty = true
}

// WillMessage returns the Will Message that is to be published to the Will Topic.
func (cm *ConnectMessage) WillMessage() []byte {
	return cm.willMessage
}

// SetWillMessage sets the Will Message that is to be published to the Will Topic.
func (cm *ConnectMessage) SetWillMessage(v []byte) {
	cm.willMessage = v

	if len(v) > 0 {
		cm.SetWillFlag(true)
	} else if len(cm.willTopic) == 0 {
		cm.SetWillFlag(false)
	}

	cm.dirty = true
}

// Username returns the username from the payload. If the User Name Flag is set to 1,
// this must be in the payload. It can be used by the Server for authentication and
// authorization.
func (cm *ConnectMessage) Username() []byte {
	return cm.username
}

// SetUsername sets the username for authentication.
func (cm *ConnectMessage) SetUsername(v []byte) {
	cm.username = v

	if len(v) > 0 {
		cm.SetUsernameFlag(true)
	} else {
		cm.SetUsernameFlag(false)
	}

	cm.dirty = true
}

// Password returns the password from the payload. If the Password Flag is set to 1,
// this must be in the payload. It can be used by the Server for authentication and
// authorization.
func (cm *ConnectMessage) Password() []byte {
	return cm.password
}

// SetPassword sets the username for authentication.
func (cm *ConnectMessage) SetPassword(v []byte) {
	cm.password = v

	if len(v) > 0 {
		cm.SetPasswordFlag(true)
	} else {
		cm.SetPasswordFlag(false)
	}

	cm.dirty = true
}

func (cm *ConnectMessage) Len() int {
	if !cm.dirty {
		return len(cm.dbuf)
	}

	ml := cm.msglen()

	if err := cm.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return cm.header.msglen() + ml
}

// For the CONNECT message, the error returned could be a ConnackReturnCode, so
// be sure to check that. Otherwise it's a generic error. If a generic error is
// returned, this Message should be considered invalid.
//
// Caller should call ValidConnackError(err) to see if the returned error is
// a Connack error. If so, caller should send the Client back the corresponding
// CONNACK message.
func (cm *ConnectMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := cm.header.decode(src[total:])
	if err != nil {
		return total + n, err
	}
	total += n

	if n, err = cm.decodeMessage(src[total:]); err != nil {
		return total + n, err
	}
	total += n

	cm.dirty = false

	return total, nil
}

func (cm *ConnectMessage) Encode(dst []byte) (int, error) {
	if !cm.dirty {
		if len(dst) < len(cm.dbuf) {
			return 0, fmt.Errorf("connect/Encode: Insufficient buffer size. Expecting %d, got %d.", len(cm.dbuf), len(dst))
		}

		return copy(dst, cm.dbuf), nil
	}

	if cm.Type() != CONNECT {
		return 0, fmt.Errorf("connect/Encode: Invalid message type. Expecting %d, got %d", CONNECT, cm.Type())
	}

	_, ok := SupportedVersions[cm.version]
	if !ok {
		return 0, ErrInvalidProtocolVersion
	}

	hl := cm.header.msglen()
	ml := cm.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("connect/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := cm.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := cm.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	n, err = cm.encodeMessage(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	return total, nil
}

func (cm *ConnectMessage) encodeMessage(dst []byte) (int, error) {
	total := 0

	n, err := writeLPBytes(dst[total:], []byte(SupportedVersions[cm.version]))
	total += n
	if err != nil {
		return total, err
	}

	dst[total] = cm.version
	total += 1

	dst[total] = cm.connectFlags
	total += 1

	binary.BigEndian.PutUint16(dst[total:], cm.keepAlive)
	total += 2

	n, err = writeLPBytes(dst[total:], cm.clientId)
	total += n
	if err != nil {
		return total, err
	}

	if cm.WillFlag() {
		n, err = writeLPBytes(dst[total:], cm.willTopic)
		total += n
		if err != nil {
			return total, err
		}

		n, err = writeLPBytes(dst[total:], cm.willMessage)
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the username string is missing.
	if cm.UsernameFlag() && len(cm.username) > 0 {
		n, err = writeLPBytes(dst[total:], cm.username)
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if cm.PasswordFlag() && len(cm.password) > 0 {
		n, err = writeLPBytes(dst[total:], cm.password)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (cm *ConnectMessage) decodeMessage(src []byte) (int, error) {
	var err error
	n, total := 0, 0

	cm.protoName, n, err = readLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	cm.version = src[total]
	total++

	if verstr, ok := SupportedVersions[cm.version]; !ok {
		return total, ErrInvalidProtocolVersion
	} else if verstr != string(cm.protoName) {
		return total, ErrInvalidProtocolVersion
	}

	cm.connectFlags = src[total]
	total++

	if cm.connectFlags&0x1 != 0 {
		return total, errors.New("connect/decodeMessage: Connect Flags reserved bit 0 is not 0")
	}

	if cm.WillQos() > QosExactlyOnce {
		return total, fmt.Errorf("connect/decodeMessage: Invalid QoS level (%d) for %s message", cm.WillQos(), cm.Name())
	}

	if !cm.WillFlag() && (cm.WillRetain() || cm.WillQos() != QosAtMostOnce) {
		return total, fmt.Errorf("connect/decodeMessage: Protocol violation: If the Will Flag (%t) is set to 0 the Will QoS (%d) and Will Retain (%t) fields MUST be set to zero", cm.WillFlag(), cm.WillQos(), cm.WillRetain())
	}

	if cm.UsernameFlag() && !cm.PasswordFlag() {
		return total, errors.New("connect/decodeMessage: Username flag is set but Password flag is not set")
	}

	if len(src[total:]) < 2 {
		return 0, fmt.Errorf("connect/decodeMessage: Insufficient buffer size. Expecting %d, got %d.", 2, len(src[total:]))
	}

	cm.keepAlive = binary.BigEndian.Uint16(src[total:])
	total += 2

	cm.clientId, n, err = readLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	// If the Client supplies a zero-byte ClientId, the Client MUST also set CleanSession to 1
	if len(cm.clientId) == 0 && !cm.CleanSession() {
		return total, ErrIdentifierRejected
	}

	// The ClientId must contain only characters 0-9, a-z, and A-Z
	// We also support ClientId longer than 23 encoded bytes
	// We do not support ClientId outside of the above characters
	if len(cm.clientId) > 0 && !cm.validClientId(cm.clientId) {
		return total, ErrIdentifierRejected
	}

	if cm.WillFlag() {
		cm.willTopic, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		cm.willMessage, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if cm.UsernameFlag() && len(src[total:]) > 0 {
		cm.username, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if cm.PasswordFlag() && len(src[total:]) > 0 {
		cm.password, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (cm *ConnectMessage) msglen() int {
	total := 0

	verstr, ok := SupportedVersions[cm.version]
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
	total += 2 + len(cm.clientId)

	// Add the will topic and will message length, and the length prefixes
	if cm.WillFlag() {
		total += 2 + len(cm.willTopic) + 2 + len(cm.willMessage)
	}

	// Add the username length
	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the user name string is missing.
	if cm.UsernameFlag() && len(cm.username) > 0 {
		total += 2 + len(cm.username)
	}

	// Add the password length
	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if cm.PasswordFlag() && len(cm.password) > 0 {
		total += 2 + len(cm.password)
	}

	return total
}

// validClientId checks the client ID, which is a slice of bytes, to see if it's valid.
// Client ID is valid if it meets the requirement from the MQTT spec:
// 		The Server MUST allow ClientIds which are between 1 and 23 UTF-8 encoded bytes in length,
//		and that contain only the characters
//
//		"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
func (cm *ConnectMessage) validClientId(cid []byte) bool {
	// Fixed https://github.com/surgemq/surgemq/issues/4
	//if len(cid) > 23 {
	//	return false
	//}

	if cm.Version() == 0x3 {
		return true
	}

	return clientIdRegexp.Match(cid)
}
