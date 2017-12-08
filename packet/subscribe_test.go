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
	"errors"

	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeMessageFields(t *testing.T) {
	m, err := New(ProtocolV311, SUBSCRIBE)
	require.NoError(t, err)

	msg, ok := m.(*Subscribe)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetPacketID(100)
	var id IDType
	id, err = msg.ID()
	require.NoError(t, err)
	require.Equal(t, IDType(100), id, "Error setting packet ID.")

	require.NoError(t, msg.AddTopic("/a/b/#/c", 1))
	require.Equal(t, 1, len(msg.topics), "Error adding topic.")
}

func TestSubscribeMessageDecode(t *testing.T) {
	buf := []byte{
		byte(SUBSCRIBE<<4) | 2,
		37,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		8, // topic name LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0, // QoS
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		1,  // QoS
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
		2, // QoS
	}

	//msg := NewSubscribeMessage()
	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*Subscribe)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, SUBSCRIBE, msg.Type(), "Error decoding message.")
	require.Equal(t, 3, len(msg.topics), "Error decoding topics.")
}

// test empty topic list
func TestSubscribeMessageDecode2(t *testing.T) {
	buf := []byte{
		byte(SUBSCRIBE<<4) | 2,
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	_, _, err := Decode(ProtocolV311, buf)

	require.Error(t, err)
}

func TestSubscribeMessageEncode(t *testing.T) {
	buf := []byte{
		byte(SUBSCRIBE<<4) | 2,
		36,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0, // QoS
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		1,  // QoS
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
		2, // QoS
	}

	m, err := New(ProtocolV311, SUBSCRIBE)
	require.NoError(t, err)

	msg, ok := m.(*Subscribe)
	require.True(t, ok, "Couldn't cast message type")

	msg.SetPacketID(7)
	msg.AddTopic("volantmq", 0)   // nolint: errcheck
	msg.AddTopic("/a/b/#/c", 1)   // nolint: errcheck
	msg.AddTopic("/a/b/#/cdd", 2) // nolint: errcheck

	dst := make([]byte, 100)
	n, err := msg.Encode(dst)
	require.NoError(t, err, "Error encoding message.")
	require.Equal(t, len(buf), n, "Error encoding message.")

	//msg1 := NewSubscribeMessage()
	var m1 Provider
	m1, n, err = Decode(ProtocolV311, dst)
	msg1, ok := m1.(*Subscribe)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, 3, len(msg1.topics), "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestSubscribeDecodeEncodeEquiv(t *testing.T) {
	buf := []byte{
		byte(SUBSCRIBE<<4) | 2,
		37,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		8, // topic name LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0, // QoS
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		1,  // QoS
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
		2, // QoS
	}

	//msg := NewSubscribeMessage()
	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*Subscribe)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(buf), n, "Raw message length does not match")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error encoding message")
	require.Equal(t, len(buf), n2, "Raw message length does not match")

	_, n3, err := Decode(ProtocolV311, dst)
	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(buf), n3, "Raw message length does not match")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestSubscribeDecodeOrder(t *testing.T) {
	buf := []byte{
		byte(SUBSCRIBE<<4) | 2,
		37,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		8, // topic name LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0, // QoS
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		1,  // QoS
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
		2, // QoS
	}

	m, n, err := Decode(ProtocolV311, buf)
	msg, ok := m.(*Subscribe)
	require.Equal(t, true, ok, "Invalid message type")
	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(buf), n, "Raw message length does not match")

	i := 0
	msg.RangeTopics(func(topic string, ops SubscriptionOptions) {
		switch i {
		case 0:
			require.Equal(t, "volantmq", topic)
			require.Equal(t, SubscriptionOptions(QoS0), ops)
		case 1:
			require.Equal(t, "/a/b/#/c", topic)
			require.Equal(t, SubscriptionOptions(QoS1), ops)
		case 2:
			require.Equal(t, "/a/b/#/cdd", topic)
			require.Equal(t, SubscriptionOptions(QoS2), ops)
		default:
			assert.Error(t, errors.New("invalid topics count"))
		}
		i++
	})
}
