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

func TestUnSubscribeMessageFields(t *testing.T) {
	m, err := NewMessage(ProtocolV311, UNSUBSCRIBE)
	require.NoError(t, err)

	msg, ok := m.(*UnSubscribeMessage)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetPacketID(100)
	id, _ := msg.PacketID()
	require.Equal(t, PacketID(100), id, "Error setting packet ID.")

	msg.AddTopic("/a/b/#/c") // nolint: errcheck
	require.Equal(t, 1, msg.Topics().Len(), "Error adding topic.")

	msg.AddTopic("/a/b/#/c") // nolint: errcheck
	require.Equal(t, 1, msg.Topics().Len(), "Error adding duplicate topic.")

	msg.RemoveTopic("/a/b/#/c")
	_, exists := msg.Topics().Get("/a/b/#/c")
	require.False(t, exists, "Topic should not exist.")

	_, exists = msg.Topics().Get("a/b")
	require.False(t, exists, "Topic should not exist.")
}

func TestUnSubscribeMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		33,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
	}

	m, n, err := Decode(ProtocolV311, msgBytes)

	msg, ok := m.(*UnSubscribeMessage)

	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, UNSUBSCRIBE, msg.Type(), "Error decoding message.")
	require.Equal(t, 3, msg.Topics().Len(), "Error decoding topics.")

	_, exists := msg.Topics().Get("surgemq")
	require.True(t, exists, "Topic 'surgemq' should exist.")

	_, exists = msg.Topics().Get("/a/b/#/c")
	require.True(t, exists, "Topic '/a/b/#/c' should exist.")

	_, exists = msg.Topics().Get("/a/b/#/cdd")
	require.True(t, exists, "Topic '/a/b/#/c' should exist.")
}

// test empty topic list
func TestUnSubscribeMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	_, _, err := Decode(ProtocolV311, msgBytes)

	require.Error(t, err)
}

func TestUnSubscribeMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		33,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
	}

	m, err := NewMessage(ProtocolV311, UNSUBSCRIBE)
	require.NoError(t, err)

	msg, ok := m.(*UnSubscribeMessage)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetPacketID(7)
	msg.AddTopic("surgemq")    // nolint: errcheck
	msg.AddTopic("/a/b/#/c")   // nolint: errcheck
	msg.AddTopic("/a/b/#/cdd") // nolint: errcheck

	dst := make([]byte, 100)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error encoding message.")
	require.Equal(t, len(msgBytes), n, "Error encoding message.")

	//msg1 := NewUnSubscribeMessage()

	var m1 Provider
	m1, n, err = Decode(ProtocolV311, dst)
	msg1, ok := m1.(*UnSubscribeMessage)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")

	_, exists := msg1.Topics().Get("surgemq")
	require.True(t, exists, "Error decoding message.")

	_, exists = msg1.Topics().Get("/a/b/#/c")
	require.True(t, exists, "Error decoding message.")

	_, exists = msg1.Topics().Get("/a/b/#/cdd")
	require.True(t, exists, "Error decoding message.")

	require.Equal(t, 3, msg.Topics().Len(), "Error decoding message.")

	//require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestUnSubscribeDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		33,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
	}

	m, n, err := Decode(ProtocolV311, msgBytes)
	msg, ok := m.(*UnSubscribeMessage)
	require.Equal(t, true, ok, "Invalid message type")
	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(msgBytes), n, "Raw message length does not match")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error encoding message.")
	require.Equal(t, len(msgBytes), n2, "Raw message length does not match")

	_, exists := msg.Topics().Get("surgemq")
	require.True(t, exists, "Topic does not exist")

	_, exists = msg.Topics().Get("/a/b/#/c")
	require.True(t, exists, "Topic does not exist")

	_, exists = msg.Topics().Get("/a/b/#/cdd")
	require.True(t, exists, "Topic does not exist")

	require.Equal(t, 3, msg.Topics().Len(), "Topics count does not match")

	_, n3, err := Decode(ProtocolV311, dst)

	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(msgBytes), n3, "Raw message length does not match")
}
