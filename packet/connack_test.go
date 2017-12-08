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

func TestConnAckMessageFields(t *testing.T) {
	m, err := New(ProtocolV311, CONNACK)
	require.NoError(t, err)

	msg, ok := m.(*ConnAck)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetSessionPresent(true)
	require.True(t, msg.SessionPresent(), "Error setting session present flag.")

	msg.SetSessionPresent(false)
	require.False(t, msg.SessionPresent(), "Error setting session present flag.")

	err = msg.SetReturnCode(CodeSuccess)
	require.NoError(t, err)

	require.Equal(t, CodeSuccess, msg.ReturnCode(), "Error setting return code.")
}

func TestConnAckMessageDecode(t *testing.T) {
	buf := []byte{
		byte(CONNACK << 4),
		2,
		0,                 // session not present
		byte(CodeSuccess), // connection accepted
	}

	m, n, err := Decode(ProtocolV311, buf)
	require.NoError(t, err)

	msg, ok := m.(*ConnAck)
	require.Equal(t, true, ok, "Invalid message type")

	require.Equal(t, len(buf), n, "Error decoding message.")
	require.False(t, msg.SessionPresent(), "Error decoding session present flag.")
	require.Equal(t, CodeSuccess, msg.ReturnCode(), "Error decoding return code.")
}

// testing wrong message length
func TestConnAckMessageDecode2(t *testing.T) {
	buf := []byte{
		byte(CONNACK << 4),
		3,
		0,                 // session not present
		byte(CodeSuccess), // connection accepted
	}

	_, _, err := Decode(ProtocolV311, buf)
	require.Error(t, err, "Error decoding message.")
}

// testing wrong message size
func TestConnAckMessageDecode3(t *testing.T) {
	buf := []byte{
		byte(CONNACK << 4),
		2,
		0, // session not present
	}

	_, _, err := Decode(ProtocolV311, buf)
	require.Error(t, err, "Error decoding message.")
}

// testing wrong reserve bits
func TestConnAckMessageDecode4(t *testing.T) {
	buf := []byte{
		byte(CONNACK << 4),
		2,
		64, // <- wrong size
		0,  // connection accepted
	}

	_, _, err := Decode(ProtocolV311, buf)
	require.Error(t, err, "Error decoding message.")
}

// testing invalid return code
func TestConnAckMessageDecode5(t *testing.T) {
	buf := []byte{
		byte(CONNACK << 4),
		2,
		0,
		6, // <- wrong code
	}

	_, _, err := Decode(ProtocolV311, buf)
	require.Error(t, err, "Error decoding message.")
}

func TestConnAckMessageEncode(t *testing.T) {
	buf := []byte{
		byte(CONNACK << 4),
		2,
		1, // session present
		0, // connection accepted
	}

	m, err := New(ProtocolV311, CONNACK)
	require.NoError(t, err)

	msg, ok := m.(*ConnAck)
	require.True(t, ok, "Couldn't cast message type")

	require.NoError(t, msg.SetReturnCode(CodeSuccess), "Couldn't set return code")
	require.Error(t, msg.SetReturnCode(CodeNoMatchingSubscribers), "Should return invalid return code")

	msg.SetSessionPresent(true)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(buf), n, "Error encoding message")
	require.Equal(t, buf, dst[:n], "Error encoding connack message")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestConnAckDecodeEncodeEquiv(t *testing.T) {
	buf := []byte{
		byte(CONNACK << 4),
		2,
		0, // session not present
		0, // connection accepted
	}

	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*ConnAck)
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

func TestConnAckEncodeEnsureSize(t *testing.T) {
	dst := make([]byte, 3)

	m, err := New(ProtocolV311, CONNACK)
	require.NoError(t, err)

	msg, ok := m.(*ConnAck)
	require.True(t, ok, "Couldn't cast message type")

	err = msg.SetReturnCode(CodeSuccess)
	require.NoError(t, err)

	_, err = msg.Encode(dst)
	require.EqualError(t, ErrInsufficientBufferSize, err.Error())
}

func TestConnAckCodeWrite(t *testing.T) {
	m, err := New(ProtocolV311, CONNACK)
	require.NoError(t, err)

	msg, ok := m.(*ConnAck)
	require.True(t, ok, "Couldn't cast message type")

	err = msg.SetReturnCode(CodeSuccess)
	require.NoError(t, err)

	var sz int
	sz, err = msg.Size()
	require.NoError(t, err)

	buf := make([]byte, sz)
	_, err = msg.Encode(buf)
	require.NoError(t, err)
}
