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

// ConnAckCode is the type representing the return code in the CONNACK message,
// returned after the initial CONNECT message
type ConnAckCode byte

const (
	// ConnectionAccepted Connection accepted
	ConnectionAccepted ConnAckCode = iota

	// ErrInvalidProtocolVersion The Server does not support the level of the MQTT protocol requested by the Client
	ErrInvalidProtocolVersion

	// ErrIdentifierRejected The Client identifier is correct UTF-8 but not allowed by the server
	ErrIdentifierRejected

	// ErrServerUnavailable The Network Connection has been made but the MQTT service is unavailable
	ErrServerUnavailable

	// ErrBadUsernameOrPassword The data in the user name or password is malformed
	ErrBadUsernameOrPassword

	// ErrNotAuthorized The Client is not authorized to connect
	ErrNotAuthorized
)

// Value returns the value of the ConnAckCode, which is just the byte representation
func (cac ConnAckCode) Value() byte {
	return byte(cac)
}

// Desc returns the description of the ConnAckCode
func (cac ConnAckCode) Desc() string {
	switch cac {
	case 0:
		return "Connection accepted"
	case 1:
		return "The Server does not support the level of the MQTT protocol requested by the Client"
	case 2:
		return "The Client identifier is correct UTF-8 but not allowed by the server"
	case 3:
		return "The Network Connection has been made but the MQTT service is unavailable"
	case 4:
		return "The data in the user name or password is malformed"
	case 5:
		return "The Client is not authorized to connect"
	}

	return ""
}

// Valid checks to see if the ConnAckCode is valid. Currently valid codes are <= 5
func (cac ConnAckCode) Valid() bool {
	return cac <= 5
}

// Error returns the corresponding error string for the ConnAckCode
func (cac ConnAckCode) Error() string {
	switch cac {
	case 0:
		return "Connection accepted"
	case 1:
		return "Connection Refused, unacceptable protocol version"
	case 2:
		return "Connection Refused, identifier rejected"
	case 3:
		return "Connection Refused, Server unavailable"
	case 4:
		return "Connection Refused, bad user name or password"
	case 5:
		return "Connection Refused, not authorized"
	}

	return "Unknown error"
}
