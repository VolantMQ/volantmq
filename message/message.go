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
	"strings"

	"github.com/troian/surgemq/buffer"
)

const (
	maxLPString uint16 = 65535
	//maxFixedHeaderLength int    = 5
	maxRemainingLength int32 = (256 * 1024 * 1024) - 1 // 256 MB
)

// Topics slice of topics
type Topics []string

// TopicsQoS is a map if topic and corresponding QoS
type TopicsQoS map[string]QosType

// SupportedVersions is a map of the version number (0x3 or 0x4) to the version string,
// "MQIsdp" for 0x3, and "MQTT" for 0x4.
var SupportedVersions = map[byte]string{
	0x3: "MQIsdp",
	0x4: "MQTT",
}

// Provider is an interface defined for all MQTT message types.
type Provider interface {
	// Desc returns a string description of the message type. For example, a
	// CONNECT message would return "Client request to connect to Server." These
	// descriptions are statically defined (copied from the MQTT spec) and cannot
	// be changed.
	Desc() string

	// Type returns the MessageType of the Message. The returned value should be one
	// of the constants defined for MessageType.
	Type() Type

	// PacketID
	PacketID() uint16

	// Encode writes the message bytes into the byte array from the argument. It
	// returns the number of bytes encoded and whether there's any errors along
	// the way. If there's any errors, then the byte slice and count should be
	// considered invalid.
	Encode([]byte) (int, error)

	// Send
	Send(to *buffer.Type) (int, error)

	// decode reads the bytes in the byte slice from the argument. It returns the
	// total number of bytes decoded, and whether there's any errors during the
	// process. The byte slice MUST NOT be modified during the duration of this
	// message being available since the byte slice is internally stored for
	// references.
	decode([]byte) (int, error)

	Len() int
}

// ValidTopic checks the topic, which is a slice of bytes, to see if it's valid. Topic is
// considered valid if it's longer than 0 bytes, and doesn't contain any wildcard characters
// such as + and #.
func ValidTopic(topic string) bool {
	return len(topic) > 0 && !strings.Contains(topic, "#") && !strings.Contains(topic, "+")
}

// ValidVersion checks to see if the version is valid. Current supported versions include 0x3 and 0x4.
func ValidVersion(v byte) bool {
	_, ok := SupportedVersions[v]
	return ok
}

// ValidConnAckError checks to see if the error is a ConnAck Error or not
func ValidConnAckError(err error) (ConnAckCode, bool) {
	if code, ok := err.(ConnAckCode); ok {
		return code, true
	}

	return 0, false
	//return err == ErrInvalidProtocolVersion || err == ErrIdentifierRejected ||
	//	err == ErrServerUnavailable || err == ErrBadUsernameOrPassword || err == ErrNotAuthorized
}

// Decode buf into message and return Provider type
func Decode(buf []byte) (Provider, int, error) {
	if len(buf) < 1 {
		return nil, 0, ErrInvalidLength
	}

	// [MQTT-2.2]
	mType := Type(buf[0] >> 4)
	msg, err := mType.New()
	if err != nil {
		return nil, 0, err
	}

	var total int

	if total, err = msg.decode(buf); err != nil {
		return nil, 0, err
	}

	return msg, total, nil
}

// Read length prefixed bytes
func readLPBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 2 {
		return nil, 0, ErrInsufficientBufferSize
	}

	var n int
	total := 0

	n = int(binary.BigEndian.Uint16(buf))
	total += 2

	if len(buf) < n {
		return nil, total, ErrInsufficientBufferSize
	}

	total += n

	return buf[2:total], total, nil
}

// Write length prefixed bytes
func writeLPBytes(buf []byte, b []byte) (int, error) {
	total, n := 0, len(b)

	if n > int(maxLPString) {
		return 0, fmt.Errorf("utils/writeLPBytes: Length (%d) greater than %d bytes", n, maxLPString)
	}

	if len(buf) < 2+n {
		return 0, ErrInsufficientBufferSize
	}

	binary.BigEndian.PutUint16(buf, uint16(n))
	total += 2

	copy(buf[total:], b)
	total += n

	return total, nil
}
