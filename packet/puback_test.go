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

func TestPubAckMessageFields(t *testing.T) {
	m, err := New(ProtocolV311, PUBACK)
	require.NoError(t, err)

	msg, ok := m.(*Ack)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetPacketID(100)

	id, _ := msg.ID()
	require.Equal(t, IDType(100), id)
}

func TestPubAckMessageDecode(t *testing.T) {
	buf := []byte{
		byte(PUBACK << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*Ack)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "decode length does not match")
	require.Equal(t, PUBACK, msg.Type(), "Message type does not match")

	id, _ := msg.ID()
	require.Equal(t, IDType(7), id, "IDType does not match")
}

// test insufficient bytes
func TestPubAckMessageDecode2(t *testing.T) {
	buf := []byte{
		byte(PUBACK << 4),
		2,
		7, // packet ID LSB (7)
	}

	_, _, err := Decode(ProtocolV311, buf)

	require.Error(t, err)
}

func TestPubAckMessageEncode(t *testing.T) {
	buf := []byte{
		byte(PUBACK << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	m, err := New(ProtocolV311, PUBACK)
	require.NoError(t, err)

	msg, ok := m.(*Ack)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetPacketID(7)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, buf, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPubAckDecodeEncodeEquiv(t *testing.T) {
	buf := []byte{
		byte(PUBACK << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*Ack)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n2, "Error decoding message.")
	require.Equal(t, buf, dst[:n2], "Error decoding message.")

	_, n3, err := Decode(ProtocolV311, dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n3, "Error decoding message.")
}
