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

package benchmark

import (
	"flag"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/service"
)

var (
	messages    = 100000
	publishers  = 1
	subscribers = 1
	size        = 1024
	topic       = []byte("test")
	qos         = byte(0)
	nap         = 10
	host        = "127.0.0.1"
	port        = 1883
	user        = "surgemq"
	pass        = "surgemq"
	version     = 4

	subdone, rcvdone, sentdone int64

	done, done2 chan struct{}

	totalSent,
	totalSentTime,
	totalRcvd,
	totalRcvdTime,
	sentSince,
	rcvdSince int64

	statMu sync.Mutex
)

func init() {
	flag.StringVar(&host, "host", host, "host to server")
	flag.IntVar(&port, "port", port, "port to server")
	flag.StringVar(&user, "user", user, "pass to server")
	flag.StringVar(&pass, "pass", pass, "user to server")
	flag.IntVar(&messages, "messages", messages, "number of messages to send")
	flag.IntVar(&publishers, "publishers", publishers, "number of publishers to start (in FullMesh, only this is used)")
	flag.IntVar(&subscribers, "subscribers", subscribers, "number of subscribers to start (in FullMesh, this is NOT used")
	flag.IntVar(&size, "size", size, "size of message payload to send, minimum 10 bytes")
	flag.IntVar(&version, "version", version, "mqtt version (4 is 3.1.1)")
	flag.Parse()
}

func runClientTest(t testing.TB, cid int, wg *sync.WaitGroup, f func(*service.Client)) {
	defer wg.Done()

	if size < 10 {
		size = 10
	}

	uri := "tcp://" + host + ":" + strconv.Itoa(port)

	c := connectToServer(t, uri, cid)
	if c == nil {
		return
	}

	if f != nil {
		f(c)
	}

	c.Disconnect()
}

func connectToServer(t testing.TB, uri string, cid int) *service.Client {
	c := &service.Client{}

	msg := newConnectMessage(cid)

	err := c.Connect(uri, msg)
	require.NoError(t, err)

	return c
}

func newSubscribeMessage(topic string, qos byte) *message.SubscribeMessage {
	msg := message.NewSubscribeMessage()
	msg.SetPacketId(1)
	msg.AddTopic([]byte(topic), qos)

	return msg
}

func newPublishMessageLarge(qos byte) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetTopic([]byte("test"))
	msg.SetPayload(make([]byte, 1024))
	msg.SetQoS(qos)

	return msg
}

func newConnectMessage(cid int) *message.ConnectMessage {
	msg := message.NewConnectMessage()
	msg.SetWillQos(1)
	msg.SetVersion(byte(version))
	msg.SetCleanSession(true)
	msg.SetClientId([]byte(fmt.Sprintf("surgemq%d", cid)))
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte("will"))
	msg.SetWillMessage([]byte("send me home"))
	msg.SetUsername([]byte(user))
	msg.SetPassword([]byte(pass))

	return msg
}
