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

package sessions

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troian/surgemq/message"
)

func TestSessionInit(t *testing.T) {
	sess := &Session{}
	cmsg := newConnectMessage()

	err := sess.Init(cmsg)
	require.NoError(t, err)
	require.Equal(t, len(sess.cBuf), cmsg.Len())
	require.Equal(t, cmsg.WillQos(), sess.CMsg.WillQos())
	require.Equal(t, cmsg.Version(), sess.CMsg.Version())
	require.Equal(t, cmsg.CleanSession(), sess.CMsg.CleanSession())
	require.Equal(t, cmsg.ClientID(), sess.CMsg.ClientID())
	require.Equal(t, cmsg.KeepAlive(), sess.CMsg.KeepAlive())
	require.Equal(t, cmsg.WillTopic(), sess.CMsg.WillTopic())
	require.Equal(t, cmsg.WillMessage(), sess.CMsg.WillMessage())
	require.Equal(t, cmsg.Username(), sess.CMsg.Username())
	require.Equal(t, cmsg.Password(), sess.CMsg.Password())
	require.Equal(t, []byte("will"), sess.Will.Topic())
	require.Equal(t, cmsg.WillQos(), sess.Will.QoS())

	sess.AddTopic("test", 1) // nolint: errcheck
	require.Equal(t, 1, len(sess.topics))

	topics, qoss, err := sess.Topics()
	require.NoError(t, err)
	require.Equal(t, 1, len(topics))
	require.Equal(t, 1, len(qoss))
	require.Equal(t, "test", topics[0])
	require.Equal(t, 1, int(qoss[0]))

	sess.RemoveTopic("test") // nolint: errcheck
	require.Equal(t, 0, len(sess.topics))
}

func TestSessionPublishAckqueue(t *testing.T) {
	sess := &Session{}
	cmsg := newConnectMessage()
	err := sess.Init(cmsg)
	require.NoError(t, err)

	for i := 0; i < 12; i++ {
		msg := newPublishMessage(uint16(i), 1)
		sess.Pub1ack.Wait(msg, nil) // nolint: errcheck
	}

	require.Equal(t, 12, sess.Pub1ack.len())

	ack1 := message.NewPubAckMessage()
	ack1.SetPacketID(1)
	sess.Pub1ack.Ack(ack1) // nolint: errcheck

	acked := sess.Pub1ack.GetAckMsg()
	require.Equal(t, 0, len(acked))

	ack0 := message.NewPubAckMessage()
	ack0.SetPacketID(0)
	sess.Pub1ack.Ack(ack0) // nolint: errcheck

	acked = sess.Pub1ack.GetAckMsg()
	require.Equal(t, 2, len(acked))
}

func newConnectMessage() *message.ConnectMessage {
	msg := message.NewConnectMessage()
	msg.SetWillQos(1)                  // nolint: errcheck
	msg.SetVersion(4)                  // nolint: errcheck
	msg.SetCleanSession(true)          // nolint: errcheck
	msg.SetClientID([]byte("surgemq")) // nolint: errcheck
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte("will"))
	msg.SetWillMessage([]byte("send me home"))
	msg.SetUsername([]byte("surgemq"))
	msg.SetPassword([]byte("verysecret"))

	return msg
}

func newPublishMessage(pktID uint16, qos byte) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetPacketID(pktID)        // nolint
	msg.SetTopic([]byte("abc"))   // nolint
	msg.SetPayload([]byte("abc")) // nolint
	msg.SetQoS(qos)               // nolint

	return msg
}
