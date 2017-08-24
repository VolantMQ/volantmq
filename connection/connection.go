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

package connection

import (
	"errors"
	"net"
	"sync"

	"time"

	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/configuration"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/subscriber"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
)

// nolint: golint
var (
	ErrOverflow    = errors.New("session: overflow")
	ErrPersistence = errors.New("session: error during persistence restore")
)

// DisconnectParams session state when stopped
type DisconnectParams struct {
	Will     bool
	ExpireAt *time.Duration
	State    *persistenceTypes.SessionMessages
}

type onDisconnect func(*DisconnectParams)

// Callbacks provided by sessions manager to signal session state
type Callbacks struct {
	// OnStop called when session stopped net connection and should be either suspended or deleted
	OnStop func(string, bool)
}

// WillConfig configures session for will messages
type WillConfig struct {
	Topic   string
	Message []byte
	Retain  bool
	QoS     message.QosType
}

// Config is system wide configuration parameters for every session
type Config struct {
	ID            string
	Username      string
	State         *persistenceTypes.SessionMessages
	Subscriber    subscriber.ConnectionProvider
	Auth          auth.SessionPermissions
	Messenger     types.TopicMessenger
	Metric        systree.Metric
	Conn          net.Conn
	OnDisconnect  onDisconnect
	ExpireIn      *time.Duration
	WillDelay     time.Duration
	KeepAlive     uint16
	SendQuota     uint16
	Clean         bool
	PreserveOrder bool
	Version       message.ProtocolVersion
}

// Type session
type Type struct {
	conn             *netConn
	pubIn            *ackQueue
	pubOut           *ackQueue
	publisher        *publisher
	flowControl      *packetsFlowControl
	onDisconnect     onDisconnect
	subscriber       subscriber.ConnectionProvider
	messenger        types.TopicMessenger
	auth             auth.SessionPermissions
	id               string
	username         string
	quit             chan struct{}
	onStart          types.Once
	onStop           types.Once
	onConnDisconnect types.Once
	started          sync.WaitGroup

	retained struct {
		lock sync.Mutex
		list []*message.PublishMessage
	}

	log struct {
		prod *zap.Logger
		dev  *zap.Logger
	}

	sendQuota   int32
	offlineQoS0 bool
	clean       bool
	version     message.ProtocolVersion
}

type unacknowledgedPublish struct {
	msg message.Provider
}

// New allocate new sessions object
func New(c *Config) (s *Type, err error) {
	s = &Type{
		id:           c.ID,
		username:     c.Username,
		auth:         c.Auth,
		subscriber:   c.Subscriber,
		messenger:    c.Messenger,
		version:      c.Version,
		clean:        c.Clean,
		onDisconnect: c.OnDisconnect,
		sendQuota:    int32(c.SendQuota),
		quit:         make(chan struct{}),
	}

	s.started.Add(1)
	s.publisher = newPublisher(s.quit)
	s.flowControl = newFlowControl(s.quit, c.PreserveOrder)

	s.log.prod = configuration.GetProdLogger().Named("connection." + s.id)
	s.log.dev = configuration.GetDevLogger().Named("connection." + s.id)

	// publisher queue is ready, assign to subscriber new online callback
	// and signal it to forward messages to online callback by creating channel
	s.subscriber.Online(s.onSubscribedPublish)
	if !s.clean && c.State != nil {
		// restore persisted state of the session if any
		s.loadPersistence(c.State) // nolint: errcheck
	}

	s.pubIn = newAckQueue(s.onReleaseIn)
	s.pubOut = newAckQueue(s.onReleaseOut)

	// Run connection
	// error check check at deferred call
	s.conn, err = newNet(
		&netConfig{
			id:           s.id,
			conn:         c.Conn,
			keepAlive:    c.KeepAlive,
			protoVersion: c.Version,
			on: onProcess{
				publish:     s.onPublish,
				ack:         s.onAck,
				subscribe:   s.onSubscribe,
				unSubscribe: s.onUnSubscribe,
				disconnect:  s.onConnectionClose,
			},
			packetsMetric: c.Metric.Packets(),
		})
	return
}

// Start signal session to start processing messages.
// Effective only first invoke
func (s *Type) Start() {
	s.onStart.Do(func() {
		s.conn.start()

		s.publisher.stopped.Add(1)
		s.publisher.started.Add(1)
		go s.publishWorker()
		s.publisher.started.Wait()

		s.started.Done()
	})
}

// Stop session. Function assumed to be invoked once server about to either shutdown, disconnect
// or session is being replaced
// Effective only first invoke
func (s *Type) Stop(reason message.ReasonCode) {
	s.onStop.Do(func() {
		s.conn.stop(&reason)
	})
}

func (s *Type) loadPersistence(state *persistenceTypes.SessionMessages) (err error) {
	defer s.publisher.cond.L.Unlock()
	s.publisher.cond.L.Lock()

	for _, d := range state.OutMessages {
		var msg message.Provider
		if msg, _, err = message.Decode(s.version, d); err != nil {
			s.log.prod.Error("Couldn't decode persisted message", zap.Error(err))
			err = ErrPersistence
			return
		}

		switch m := msg.(type) {
		case *message.PublishMessage:
			if m.QoS() == message.QoS2 && m.Dup() {
				s.log.prod.Error("3: QoS2 DUP")
			}
		}

		s.publisher.messages.PushFront(msg)
	}

	for _, d := range state.UnAckMessages {
		var msg message.Provider
		if msg, _, err = message.Decode(s.version, d); err != nil {
			s.log.prod.Error("Couldn't decode persisted message", zap.Error(err))
			err = ErrPersistence
			return
		}

		s.publisher.messages.PushFront(&unacknowledgedPublish{msg: msg})
	}

	return
}

// onSubscribedPublish is the method that gets added to the topic subscribers list by the
// processSubscribe() method. When the server finishes the ack cycle for a
// PUBLISH message, it will call the subscriber, which is this method.
//
// For the server, when this method is called, it means there's a message that
// should be published to the client on the other end of this connection. So we
// will call publish() to send the message.
func (s *Type) onSubscribedPublish(msg *message.PublishMessage) error {
	_m, _ := message.NewMessage(s.version, message.PUBLISH)
	m := _m.(*message.PublishMessage)

	m.SetQoS(msg.QoS())     // nolint: errcheck
	m.SetTopic(msg.Topic()) // nolint: errcheck
	m.SetPayload(msg.Payload())

	// [MQTT-3.3.1-9]
	m.SetRetain(false)

	// [MQTT-3.3.1-3]
	m.SetDup(false)

	s.publisher.pushBack(m)

	return nil
}

// forward PUBLISH message to topics manager which takes care about subscribers
func (s *Type) publishToTopic(msg *message.PublishMessage) error {
	// [MQTT-3.3.1.3]
	if msg.Retain() {
		if err := s.messenger.Retain(msg); err != nil {
			s.log.prod.Error("Error retaining message", zap.String("ClientID", s.id), zap.Error(err))
		}

		// [MQTT-3.3.1-7]
		if msg.QoS() == message.QoS0 {
			_m, _ := message.NewMessage(s.version, message.PUBLISH)
			m := _m.(*message.PublishMessage)
			m.SetQoS(msg.QoS())     // nolint: errcheck
			m.SetTopic(msg.Topic()) // nolint: errcheck
			s.retained.lock.Lock()
			s.retained.list = append(s.retained.list, m)
			s.retained.lock.Unlock()
		}
	}

	if err := s.messenger.Publish(msg); err != nil {
		s.log.prod.Error("Couldn't publish", zap.String("ClientID", s.id), zap.Error(err))
	}

	return nil
}

// onReleaseIn ack process for incoming messages
func (s *Type) onReleaseIn(msg message.Provider) {
	switch m := msg.(type) {
	case *message.PublishMessage:
		s.publishToTopic(m) // nolint: errcheck
	}
}

// onReleaseOut process messages that required ack cycle
// onAckTimeout if publish message has not been acknowledged withing specified ackTimeout
// server should mark it as a dup and send again
func (s *Type) onReleaseOut(msg message.Provider) {
	switch msg.Type() {
	case message.PUBACK:
		fallthrough
	case message.PUBCOMP:
		id, _ := msg.PacketID()
		s.flowControl.release(id)
	}
}

// publishWorker publish messages coming from subscribed topics
func (s *Type) publishWorker() {
	//defer s.Stop(message.CodeSuccess)
	defer s.publisher.stopped.Done()
	s.publisher.started.Done()

	value := s.publisher.waitForMessage()
	for ; value != nil; value = s.publisher.waitForMessage() {
		var msg message.Provider
		switch m := value.(type) {
		case *message.PublishMessage:
			if m.QoS() != message.QoS0 {
				if id, err := s.flowControl.acquire(); err == nil {
					m.SetPacketID(id)
					s.pubOut.store(m)
				} else {
					// if acquire id returned error session is about to exit. Queue message back and get away
					s.publisher.pushBack(m)
					return
				}
			}
			msg = m
		case *unacknowledgedPublish:
			msg = m.msg
			s.pubOut.store(msg)
		}

		if _, err := s.conn.WriteMessage(msg, false); err != nil {
			// Couldn't deliver message to client thus requeue it back
			// Error during write means connection has been closed/timed-out etc
			return
		}
	}
}
