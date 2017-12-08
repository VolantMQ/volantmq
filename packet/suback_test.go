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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubAckMessageFields(t *testing.T) {
	m, err := New(ProtocolV311, SUBACK)
	require.NoError(t, err)

	msg, ok := m.(*SubAck)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetPacketID(100)
	id, _ := msg.ID()
	require.Equal(t, IDType(100), id, "Error setting packet ID.")

	msg.AddReturnCode(1) // nolint: errcheck
	require.Equal(t, 1, len(msg.ReturnCodes()), "Error adding return code.")

	err = msg.AddReturnCode(0x90)
	require.Error(t, err)
}

func TestSubAckMessageDecode(t *testing.T) {
	buf := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x80, // return code 4
	}

	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*SubAck)
	require.Equal(t, true, ok, "Invalid message type")

	if err != nil {
		t.Log(err)
	}
	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, SUBACK, msg.Type(), "Error decoding message.")
	require.Equal(t, 4, len(msg.ReturnCodes()), "Error adding return code.")
}

// test with wrong return code
func TestSubAckMessageDecode2(t *testing.T) {
	buf := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x81, // return code 4
	}

	_, _, err := Decode(ProtocolV311, buf)
	require.Error(t, err)
}

func TestSubAckMessageEncode(t *testing.T) {
	buf := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x80, // return code 4
	}

	m, err := New(ProtocolV311, SUBACK)
	require.NoError(t, err)

	msg, ok := m.(*SubAck)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetPacketID(7)
	msg.AddReturnCode(0)    // nolint: errcheck
	msg.AddReturnCode(1)    // nolint: errcheck
	msg.AddReturnCode(2)    // nolint: errcheck
	msg.AddReturnCode(0x80) // nolint: errcheck

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error encoding message.")
	require.Equal(t, len(buf), n, "Encoded length does not match")
	require.Equal(t, buf, dst[:n], "Raw message does not match")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestSubAckDecodeEncodeEquiv(t *testing.T) {
	buf := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x80, // return code 4
	}

	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*SubAck)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error encoding message.")
	require.Equal(t, len(buf), n2, "Error encoding message.")
	require.Equal(t, buf, dst[:n2], "Error encoding message.")

	_, n3, err := Decode(ProtocolV311, dst)

	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(buf), n3, "Error decoding message")
}
