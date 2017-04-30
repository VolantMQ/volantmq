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

package service

import (
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/sessions"
	"github.com/troian/surgemq/topics"
)

const (
	minKeepAlive = 30
)

// Client is a library implementation of the MQTT client that, as best it can, complies
// with the MQTT 3.1 and 3.1.1 specs.
type Client struct {
	// The number of seconds to keep the connection live if there's no data.
	// If not set then default to 5 mins.
	KeepAlive int

	// The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// The number of seconds to wait for any ACK messages before failing.
	// If not set then default to 20 seconds.
	AckTimeout int

	// The number of times to retry sending a packet if ACK is not received.
	// If no set then default to 3 retries.
	TimeoutRetries int

	svc *service
}

// Connect is for MQTT clients to open a connection to a remote server. It needs to
// know the URI, e.g., "tcp://127.0.0.1:1883", so it knows where to connect to. It also
// needs to be supplied with the MQTT CONNECT message.
func (c *Client) Connect(uri string, msg *message.ConnectMessage) (err error) {
	c.checkConfiguration()

	if msg == nil {
		return fmt.Errorf("msg is nil")
	}

	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	if u.Scheme != "tcp" {
		return ErrInvalidConnectionType
	}

	conn, err := net.Dial(u.Scheme, u.Host)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			conn.Close() // nolint: errcheck
		}
	}()

	if msg.KeepAlive() < minKeepAlive {
		msg.SetKeepAlive(minKeepAlive)
	}

	if err = writeMessage(conn, msg); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(c.ConnectTimeout))) // nolint: errcheck

	resp, err := getConnAckMessage(conn)
	if err != nil {
		return err
	}

	if resp.ReturnCode() != message.ConnectionAccepted {
		return resp.ReturnCode()
	}

	c.svc = &service{
		id:     atomic.AddUint64(&gSvcID, 1),
		client: true,
		conn:   conn,

		keepAlive:      int(msg.KeepAlive()),
		connectTimeout: c.ConnectTimeout,
		ackTimeout:     c.AckTimeout,
		timeoutRetries: c.TimeoutRetries,
	}

	err = c.getSession(c.svc, msg, resp)
	if err != nil {
		return err
	}

	p := topics.NewMemProvider()
	topics.Register(c.svc.sess.ID(), p)

	c.svc.topicsMgr, err = topics.NewManager(c.svc.sess.ID())
	if err != nil {
		return err
	}

	if err := c.svc.start(); err != nil {
		c.svc.stop()
		return err
	}

	c.svc.inStat.increment(int64(msg.Len()))
	c.svc.outStat.increment(int64(resp.Len()))

	return nil
}

// Publish sends a single MQTT PUBLISH message to the server. On completion, the
// supplied OnCompleteFunc is called. For QOS 0 messages, onComplete is called
// immediately after the message is sent to the outgoing buffer. For QOS 1 messages,
// onComplete is called when PUBACK is received. For QOS 2 messages, onComplete is
// called after the PUBCOMP message is received.
func (c *Client) Publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	return c.svc.publish(msg, onComplete)
}

// Subscribe sends a single SUBSCRIBE message to the server. The SUBSCRIBE message
// can contain multiple topics that the client wants to subscribe to. On completion,
// which is when the client receives a SUBACK messsage back from the server, the
// supplied onComplete funciton is called.
//
// When messages are sent to the client from the server that matches the topics the
// client subscribed to, the onPublish function is called to handle those messages.
// So in effect, the client can supply different onPublish functions for different
// topics.
func (c *Client) Subscribe(msg *message.SubscribeMessage, onComplete OnCompleteFunc, onPublish OnPublishFunc) error {
	return c.svc.subscribe(msg, onComplete, onPublish)
}

// UnSubscribe sends a single UNSUBSCRIBE message to the server. The UNSUBSCRIBE
// message can contain multiple topics that the client wants to unsubscribe. On
// completion, which is when the client receives a UNSUBACK message from the server,
// the supplied onComplete function is called. The client will no longer handle
// messages from the server for those unsubscribed topics.
func (c *Client) UnSubscribe(msg *message.UnSubscribeMessage, onComplete OnCompleteFunc) error {
	return c.svc.unSubscribe(msg, onComplete)
}

// Ping sends a single PINGREQ message to the server. PINGREQ/PINGRESP messages are
// mainly used by the client to keep a heartbeat to the server so the connection won't
// be dropped.
func (c *Client) Ping(onComplete OnCompleteFunc) error {
	return c.svc.ping(onComplete)
}

// Disconnect sends a single DISCONNECT message to the server. The client immediately
// terminates after the sending of the DISCONNECT message.
func (c *Client) Disconnect() {
	//msg := message.NewDisconnectMessage()
	c.svc.stop()
}

func (c *Client) getSession(svc *service, req *message.ConnectMessage, resp *message.ConnAckMessage) error {
	//id := string(req.ClientId())
	svc.sess = &sessions.Session{}
	return svc.sess.Init(req)
}

func (c *Client) checkConfiguration() {
	if c.KeepAlive == 0 {
		c.KeepAlive = DefaultKeepAlive
	}

	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}

	if c.AckTimeout == 0 {
		c.AckTimeout = DefaultAckTimeout
	}

	if c.TimeoutRetries == 0 {
		c.TimeoutRetries = DefaultTimeoutRetries
	}
}
