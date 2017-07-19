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
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	lpStrings = []string{
		"this is a test",
		"hope it succeeds",
		"but just in case",
		"send me your millions",
		"",
	}

	lpStringBytes = []byte{
		0x0, 0xe, 't', 'h', 'i', 's', ' ', 'i', 's', ' ', 'a', ' ', 't', 'e', 's', 't',
		0x0, 0x10, 'h', 'o', 'p', 'e', ' ', 'i', 't', ' ', 's', 'u', 'c', 'c', 'e', 'e', 'd', 's',
		0x0, 0x10, 'b', 'u', 't', ' ', 'j', 'u', 's', 't', ' ', 'i', 'n', ' ', 'c', 'a', 's', 'e',
		0x0, 0x15, 's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'y', 'o', 'u', 'r', ' ', 'm', 'i', 'l', 'l', 'i', 'o', 'n', 's',
		0x0, 0x0,
	}

	// nolint: megacheck
	msgBytes = []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}
)

func TestReadLPBytes(t *testing.T) {
	total := 0

	for _, str := range lpStrings {
		b, n, err := ReadLPBytes(lpStringBytes[total:])

		require.NoError(t, err)
		require.Equal(t, str, string(b))
		require.Equal(t, len(str)+2, n)

		total += n
	}

	buf := make([]byte, 1)
	_, _, err := ReadLPBytes(buf)
	require.EqualError(t, err, ErrInsufficientDataSize.Error())

	buf = make([]byte, 3)
	buf[0] = 2

	_, _, err = ReadLPBytes(buf)
	require.EqualError(t, err, ErrInsufficientDataSize.Error())
}

func TestWriteLPBytes(t *testing.T) {
	total := 0
	buf := make([]byte, 1000)

	for _, str := range lpStrings {
		n, err := WriteLPBytes(buf[total:], []byte(str))

		require.NoError(t, err)
		require.Equal(t, 2+len(str), n)

		total += n
	}

	require.Equal(t, lpStringBytes, buf[:total])

	testString := []byte("blablabla")
	buf = make([]byte, 4)

	_, err := WriteLPBytes(buf, testString)
	require.EqualError(t, ErrInsufficientBufferSize, err.Error())

	testString = make([]byte, int(maxLPString)+1)
	_, err = WriteLPBytes(buf, testString)
	require.EqualError(t, ErrInvalidLPStringSize, err.Error())
}

func TestMessageTypes(t *testing.T) {
	if CONNECT != 1 ||
		CONNACK != 2 ||
		PUBLISH != 3 ||
		PUBACK != 4 ||
		PUBREC != 5 ||
		PUBREL != 6 ||
		PUBCOMP != 7 ||
		SUBSCRIBE != 8 ||
		SUBACK != 9 ||
		UNSUBSCRIBE != 10 ||
		UNSUBACK != 11 ||
		PINGREQ != 12 ||
		PINGRESP != 13 ||
		DISCONNECT != 14 ||
		AUTH != 15 {
		t.Errorf("Message types have invalid code")
	}
}

func TestQosCodes(t *testing.T) {
	if QoS0 != 0 || QoS1 != 1 || QoS2 != 2 {
		t.Errorf("QOS codes invalid")
	}
}

//func TestConnackReturnCodes(t *testing.T) {
//	require.Equal(t, ErrInvalidProtocolVersion.Error(), ConnAckCode(1).Error(), "Incorrect ConnackCode error value.")
//
//	require.Equal(t, ErrIdentifierRejected.Error(), ConnAckCode(2).Error(), "Incorrect ConnackCode error value.")
//
//	require.Equal(t, ErrServerUnavailable.Error(), ConnAckCode(3).Error(), "Incorrect ConnackCode error value.")
//
//	require.Equal(t, ErrBadUsernameOrPassword.Error(), ConnAckCode(4).Error(), "Incorrect ConnackCode error value.")
//
//	require.Equal(t, ErrNotAuthorized.Error(), ConnAckCode(5).Error(), "Incorrect ConnackCode error value.")
//
//	_, ok := ValidConnAckError(ErrInvalidProtocolVersion)
//	require.True(t, ok)
//
//	_, ok = ValidConnAckError(ErrIdentifierRejected)
//	require.True(t, ok)
//
//	_, ok = ValidConnAckError(ErrServerUnavailable)
//	require.True(t, ok)
//
//	_, ok = ValidConnAckError(ErrBadUsernameOrPassword)
//	require.True(t, ok)
//
//	_, ok = ValidConnAckError(ErrNotAuthorized)
//	require.True(t, ok)
//
//	_, ok = ValidConnAckError(errors.New("bla bla bla bla"))
//	require.False(t, ok)
//
//}

func TestFixedHeaderFlags(t *testing.T) {
	type detail struct {
		name  string
		flags byte
	}

	details := map[PacketType]detail{
		RESERVED:    {"RESERVED", 0},
		CONNECT:     {"CONNECT", 0},
		CONNACK:     {"CONNACK", 0},
		PUBLISH:     {"PUBLISH", 0},
		PUBACK:      {"PUBACK", 0},
		PUBREC:      {"PUBREC", 0},
		PUBREL:      {"PUBREL", 2},
		PUBCOMP:     {"PUBCOMP", 0},
		SUBSCRIBE:   {"SUBSCRIBE", 2},
		SUBACK:      {"SUBACK", 0},
		UNSUBSCRIBE: {"UNSUBSCRIBE", 2},
		UNSUBACK:    {"UNSUBACK", 0},
		PINGREQ:     {"PINGREQ", 0},
		PINGRESP:    {"PINGRESP", 0},
		DISCONNECT:  {"DISCONNECT", 0},
		AUTH:        {"AUTH", 0},
	}

	for m, d := range details {
		if m.Name() != d.name {
			t.Errorf("Name mismatch. Expecting %s, got %s.", d.name, m.Name())
		}

		if m.DefaultFlags() != d.flags {
			t.Errorf("Flag mismatch for %s. Expecting %d, got %d.", m.Name(), d.flags, m.DefaultFlags())
		}
	}
}

func TestSupportedVersions(t *testing.T) {
	for k, v := range SupportedVersions {
		if k == 0x03 && v != "MQIsdp" {
			t.Errorf("Protocol version and name mismatch. Expect %s, got %s.", "MQIsdp", v)
		}
		if k == 0x04 && v != "MQTT" {
			t.Errorf("Protocol version and name mismatch. Expect %s, got %s.", "MQTT", v)
		}
		if k == 0x05 && v != "MQTT" {
			t.Errorf("Protocol version and name mismatch. Expect %s, got %s.", "MQTT", v)
		}
	}

	require.True(t, ProtocolVersion(0x03).IsValid())
	require.True(t, ProtocolVersion(0x04).IsValid())
	require.True(t, ProtocolVersion(0x05).IsValid())
}

func TestMessageDecode(t *testing.T) {
	var buf []byte

	_, n, err := Decode(ProtocolV311, buf)
	require.Equal(t, 0, n)
	require.EqualError(t, err, ErrInsufficientBufferSize.Error())

	buf = make([]byte, 1)
	buf[0] = 0x0F << offsetHeaderType

	_, n, err = Decode(ProtocolV311, buf)
	require.Error(t, err)
	require.Equal(t, 0, n)
}
