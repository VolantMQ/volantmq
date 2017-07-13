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

func TestSubscribeMessageFields(t *testing.T) {
	msg := NewSubscribeMessage()

	msg.SetPacketID(100)
	require.Equal(t, 100, int(msg.PacketID()), "Error setting packet ID.")

	msg.AddTopic("/a/b/#/c", 1) // nolint: errcheck
	require.Equal(t, 1, len(msg.Topics()), "Error adding topic.")

	require.False(t, msg.TopicExists("a/b"), "Topic should not exist.")

	msg.RemoveTopic("/a/b/#/c")
	require.False(t, msg.TopicExists("/a/b/#/c"), "Topic should not exist.")
}

func TestSubscribeMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(SUBSCRIBE<<4) | 2,
		36,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
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
	m, n, err := Decode(msgBytes)
	msg, ok := m.(*SubscribeMessage)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, SUBSCRIBE, msg.Type(), "Error decoding message.")
	require.Equal(t, 3, len(msg.Topics()), "Error decoding topics.")
	require.True(t, msg.TopicExists("surgemq"), "Topic 'surgemq' should exist.")
	require.Equal(t, 0, int(msg.TopicQos("surgemq")), "Incorrect topic qos.")
	require.True(t, msg.TopicExists("/a/b/#/c"), "Topic '/a/b/#/c' should exist.")
	require.Equal(t, 1, int(msg.TopicQos("/a/b/#/c")), "Incorrect topic qos.")
	require.True(t, msg.TopicExists("/a/b/#/cdd"), "Topic '/a/b/#/c' should exist.")
	require.Equal(t, 2, int(msg.TopicQos("/a/b/#/cdd")), "Incorrect topic qos.")
}

// test empty topic list
func TestSubscribeMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(SUBSCRIBE<<4) | 2,
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	//msg := NewSubscribeMessage()
	_, _, err := Decode(msgBytes)

	require.Error(t, err)
}

func TestSubscribeMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(SUBSCRIBE<<4) | 2,
		36,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
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

	msg := NewSubscribeMessage()
	msg.SetPacketID(7)
	msg.AddTopic("surgemq", 0)    // nolint: errcheck
	msg.AddTopic("/a/b/#/c", 1)   // nolint: errcheck
	msg.AddTopic("/a/b/#/cdd", 2) // nolint: errcheck

	dst := make([]byte, 100)
	n, err := msg.Encode(dst)
	require.NoError(t, err, "Error encoding message.")
	require.Equal(t, len(msgBytes), n, "Error encoding message.")

	//msg1 := NewSubscribeMessage()
	var m1 Provider
	m1, n, err = Decode(dst)
	msg1, ok := m1.(*SubscribeMessage)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")

	exists := msg1.TopicExists("surgemq")
	require.Equal(t, true, exists, "Error decoding message.")
	qos := msg1.TopicQos("surgemq")
	require.Equal(t, QosType(0), qos, "Error decoding message.")

	exists = msg1.TopicExists("/a/b/#/c")
	require.Equal(t, true, exists, "Error decoding message.")
	qos = msg1.TopicQos("/a/b/#/c")
	require.Equal(t, QosType(1), qos, "Error decoding message.")

	exists = msg1.TopicExists("/a/b/#/cdd")
	require.Equal(t, true, exists, "Error decoding message.")
	qos = msg1.TopicQos("/a/b/#/cdd")
	require.Equal(t, QosType(2), qos, "Error decoding message.")

	topics := msg1.Topics()
	require.Equal(t, 3, len(topics), "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestSubscribeDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(SUBSCRIBE<<4) | 2,
		36,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
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
	m, n, err := Decode(msgBytes)
	msg, ok := m.(*SubscribeMessage)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(msgBytes), n, "Raw message length does not match")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error encoding message")
	require.Equal(t, len(msgBytes), n2, "Raw message length does not match")

	exists := msg.TopicExists("surgemq")
	require.Equal(t, true, exists, "Required topic does not exist")
	qos := msg.TopicQos("surgemq")
	require.Equal(t, QosType(0), qos, "Invalid QoS for topic")

	exists = msg.TopicExists("/a/b/#/c")
	require.Equal(t, true, exists, "Required topic does not exist")
	qos = msg.TopicQos("/a/b/#/c")
	require.Equal(t, QosType(1), qos, "Invalid QoS for topic")

	exists = msg.TopicExists("/a/b/#/cdd")
	require.Equal(t, true, exists, "Required topic does not exist")
	qos = msg.TopicQos("/a/b/#/cdd")
	require.Equal(t, QosType(2), qos, "Invalid QoS for topic")

	_, n3, err := Decode(dst)

	require.NoError(t, err, "Error decoding message")
	require.Equal(t, len(msgBytes), n3, "Raw message length does not match")
}
