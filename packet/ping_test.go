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

func TestPingReqMessageDecode(t *testing.T) {
	buf := []byte{
		byte(PINGREQ << 4),
		0,
	}

	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*PingReq)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, PINGREQ, msg.Type(), "Error decoding message.")
}

func TestPingReqMessageEncode(t *testing.T) {
	buf := []byte{
		byte(PINGREQ << 4),
		0,
	}

	m, err := New(ProtocolV311, PINGREQ)
	require.NoError(t, err)

	msg, ok := m.(*PingReq)
	require.True(t, ok, "Couldn't cast message type")

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, buf, dst[:n], "Error decoding message.")
}

func TestPingRespMessageDecode(t *testing.T) {
	buf := []byte{
		byte(PINGRESP << 4),
		0,
	}

	msg, n, err := Decode(ProtocolV311, buf)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, PINGRESP, msg.Type(), "Error decoding message.")
}

func TestPingRespMessageEncode(t *testing.T) {
	buf := []byte{
		byte(PINGRESP << 4),
		0,
	}

	m, err := New(ProtocolV311, PINGRESP)
	require.NoError(t, err)

	msg, ok := m.(*PingResp)
	require.True(t, ok, "Couldn't cast message type")
	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, buf, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPingReqDecodeEncodeEquiv(t *testing.T) {
	buf := []byte{
		byte(PINGREQ << 4),
		0,
	}

	msg, n, err := Decode(ProtocolV311, buf)

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

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPingRespDecodeEncodeEquiv(t *testing.T) {
	buf := []byte{
		byte(PINGRESP << 4),
		0,
	}

	msg, n, err := Decode(ProtocolV311, buf)

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
