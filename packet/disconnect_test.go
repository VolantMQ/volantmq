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

func TestDisconnectMessageDecode(t *testing.T) {
	buf := []byte{
		byte(DISCONNECT << 4),
		0,
	}

	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*Disconnect)
	require.NoError(t, err, "Error decoding message.")

	require.Equal(t, true, ok, "Invalid message type")

	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, DISCONNECT, msg.Type(), "Error decoding message.")

	buf = []byte{
		byte(DISCONNECT << 4),
		1,
		0,
	}

	_, _, err = Decode(ProtocolV50, buf)
	require.EqualError(t, CodeMalformedPacket, err.Error())

	_, _, err = Decode(ProtocolV311, buf)
	require.EqualError(t, CodeRefusedServerUnavailable, err.Error())
}

func TestDisconnectMessageEncode(t *testing.T) {
	buf := []byte{
		byte(DISCONNECT << 4),
		0,
	}

	msg, err := New(ProtocolV311, DISCONNECT)
	require.NoError(t, err)
	require.NotNil(t, msg)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, buf, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestDisconnectDecodeEncodeEquiv(t *testing.T) {
	buf := []byte{
		byte(DISCONNECT << 4),
		0,
	}

	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*Disconnect)
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
