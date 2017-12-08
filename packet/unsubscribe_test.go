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

func TestUnSubscribeMessageFields(t *testing.T) {
	m, err := New(ProtocolV311, UNSUBSCRIBE)
	require.NoError(t, err)

	msg, ok := m.(*UnSubscribe)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetPacketID(100)
	id, _ := msg.ID()
	require.Equal(t, IDType(100), id, "Error setting packet ID.")

	msg.AddTopic("/a/b/#/c") // nolint: errcheck
	require.Equal(t, 1, len(msg.topics), "Error adding topic.")
	//
	//msg.AddTopic("/a/b/#/c") // nolint: errcheck
	//require.Equal(t, 1, len(msg.topics), "Error adding duplicate topic.")
}

func TestUnSubscribeMessageDecode(t *testing.T) {
	buf := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		34,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		8, // topic name LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
	}

	m, n, err := Decode(ProtocolV311, buf)
	require.NoError(t, err, "Error decoding message.")
	msg, ok := m.(*UnSubscribe)

	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, UNSUBSCRIBE, msg.Type(), "Error decoding message.")
	require.Equal(t, 3, len(msg.topics), "Error decoding topics.")
}

// test empty topic list
func TestUnSubscribeMessageDecode2(t *testing.T) {
	buf := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	_, _, err := Decode(ProtocolV311, buf)

	require.Error(t, err)
}

func TestUnSubscribeMessageEncode(t *testing.T) {
	buf := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		33,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
	}

	m, err := New(ProtocolV311, UNSUBSCRIBE)
	require.NoError(t, err)

	msg, ok := m.(*UnSubscribe)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetPacketID(7)
	msg.AddTopic("volantmq")   // nolint: errcheck
	msg.AddTopic("/a/b/#/c")   // nolint: errcheck
	msg.AddTopic("/a/b/#/cdd") // nolint: errcheck

	dst := make([]byte, 100)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error encoding message.")
	require.Equal(t, len(buf), n, "Error encoding message.")

	//msg1 := NewUnSubscribeMessage()

	var m1 Provider
	m1, n, err = Decode(ProtocolV311, dst)
	msg1, ok := m1.(*UnSubscribe)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, 3, len(msg1.topics), "Error decoding message.")

	//require.Equal(t, buf, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestUnSubscribeDecodeEncodeEquiv(t *testing.T) {
	buf := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		34,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		8, // topic name LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
	}

	m, n, err := Decode(ProtocolV311, buf)
	require.NoError(t, err, "Error decoding message.")
	msg, ok := m.(*UnSubscribe)
	require.Equal(t, true, ok, "Invalid message type")
	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(buf), n, "Raw message length does not match")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error encoding message.")
	require.Equal(t, len(buf), n2, "Raw message length does not match")
	require.Equal(t, 3, len(msg.topics), "Topics count does not match")

	_, n3, err := Decode(ProtocolV311, dst)

	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(buf), n3, "Raw message length does not match")
}
