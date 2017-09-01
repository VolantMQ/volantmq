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
	"github.com/troian/surgemq/packet"
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
	QoS     packet.QosType
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
	Version       packet.ProtocolVersion
}

// Type session
type Type struct {
	conn             net.Conn
	pubIn            *ackQueue
	pubOut           *ackQueue
	flowControl      *packetsFlowControl
	tx               *transmitter
	rx               *receiver
	onDisconnect     onDisconnect
	subscriber       subscriber.ConnectionProvider
	messenger        types.TopicMessenger
	auth             auth.SessionPermissions
	id               string
	username         string
	quit             chan struct{}
	onStart          types.Once
	onConnDisconnect types.Once

	retained struct {
		lock sync.Mutex
		list []*packet.Publish
	}

	log         *zap.Logger
	sendQuota   int32
	offlineQoS0 bool
	clean       bool
	version     packet.ProtocolVersion
	will        bool
}

type unacknowledgedPublish struct {
	msg packet.Provider
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
		conn:         c.Conn,
		onDisconnect: c.OnDisconnect,
		sendQuota:    int32(c.SendQuota),
		quit:         make(chan struct{}),
		will:         true,
	}

	s.pubIn = newAckQueue(s.onReleaseIn)
	s.pubOut = newAckQueue(s.onReleaseOut)
	s.flowControl = newFlowControl(s.quit, c.PreserveOrder)
	s.log = configuration.GetProdLogger().Named("connection." + s.id)

	s.tx = newTransmitter(&transmitterConfig{
		quit:         s.quit,
		log:          s.log,
		id:           s.id,
		pubIn:        s.pubIn,
		pubOut:       s.pubOut,
		flowControl:  s.flowControl,
		conn:         s.conn,
		onDisconnect: s.onConnectionClose,
	})

	s.rx = newReceiver(&receiverConfig{
		quit:         s.quit,
		conn:         s.conn,
		onDisconnect: s.onConnectionClose,
		onPacket:     s.processIncoming,
		version:      s.version,
		will:         &s.will,
	})

	if c.KeepAlive > 0 {
		s.rx.keepAlive = time.Second * time.Duration(c.KeepAlive)
		s.rx.keepAlive = s.rx.keepAlive + (s.rx.keepAlive / 2)
	}

	// transmitter queue is ready, assign to subscriber new online callback
	// and signal it to forward messages to online callback by creating channel
	s.subscriber.Online(s.onSubscribedPublish)
	if !s.clean && c.State != nil {
		// restore persisted state of the session if any
		s.loadPersistence(c.State) // nolint: errcheck
	}

	return
}

// Start run connection
func (s *Type) Start() {
	s.onStart.Do(func() {
		s.tx.run()
		s.rx.run()
	})
}

// Stop session. Function assumed to be invoked once server about to either shutdown, disconnect
// or session is being replaced
// Effective only first invoke
func (s *Type) Stop(reason packet.ReasonCode) {
	s.onConnectionClose(true)
}

func (s *Type) processIncoming(p packet.Provider) error {
	var err error
	var resp packet.Provider

	switch pkt := p.(type) {
	case *packet.Publish:
		resp = s.onPublish(pkt)
	case *packet.Ack:
		resp = s.onAck(pkt)
	case *packet.Subscribe:
		resp = s.onSubscribe(pkt)
	case *packet.UnSubscribe:
		resp = s.onUnSubscribe(pkt)
	case *packet.PingReq:
		// For PINGREQ message, we should send back PINGRESP
		mR, _ := packet.NewMessage(s.version, packet.PINGRESP)
		resp, _ = mR.(*packet.PingResp)
	case *packet.Disconnect:
		// For DISCONNECT message, we should quit without sending Will
		s.will = false

		//if s.version == packet.ProtocolV50 {
		//	// FIXME: CodeRefusedBadUsernameOrPassword has same id as CodeDisconnectWithWill
		//	if m.ReasonCode() == packet.CodeRefusedBadUsernameOrPassword {
		//		s.will = true
		//	}
		//
		//	expireIn := time.Duration(0)
		//	if val, e := m.PropertyGet(packet.PropertySessionExpiryInterval); e == nil {
		//		expireIn = time.Duration(val.(uint32))
		//	}
		//
		//	// If the Session Expiry Interval in the CONNECT packet was zero, then it is a Protocol Error to set a non-
		//	// zero Session Expiry Interval in the DISCONNECT packet sent by the Client. If such a non-zero Session
		//	// Expiry Interval is received by the Server, it does not treat it as a valid DISCONNECT packet. The Server
		//	// uses DISCONNECT with Reason Code 0x82 (Protocol Error) as described in section 4.13.
		//	if s.expireIn != nil && *s.expireIn == 0 && expireIn != 0 {
		//		m, _ := packet.NewMessage(packet.ProtocolV50, packet.DISCONNECT)
		//		msg, _ := m.(*packet.Disconnect)
		//		msg.SetReasonCode(packet.CodeProtocolError)
		//		s.WriteMessage(msg, true) // nolint: errcheck
		//	}
		//}
		//return
		err = errors.New("disconnect")
	default:
		s.log.Error("Unsupported incoming message type",
			zap.String("ClientID", s.id),
			zap.String("type", p.Type().Name()))
		return nil
	}

	if resp != nil {
		s.tx.sendPacket(resp)
	}

	return err
}

func (s *Type) loadPersistence(state *persistenceTypes.SessionMessages) (err error) {
	for _, d := range state.OutMessages {
		var msg packet.Provider
		if msg, _, err = packet.Decode(s.version, d); err != nil {
			s.log.Error("Couldn't decode persisted message", zap.Error(err))
			err = ErrPersistence
			return
		}

		s.tx.loadFront(msg)
	}

	for _, d := range state.UnAckMessages {
		var msg packet.Provider
		if msg, _, err = packet.Decode(s.version, d); err != nil {
			s.log.Error("Couldn't decode persisted message", zap.Error(err))
			err = ErrPersistence
			return
		}

		s.tx.loadFront(&unacknowledgedPublish{msg: msg})
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
func (s *Type) onSubscribedPublish(p *packet.Publish) error {
	_pkt, _ := packet.NewMessage(s.version, packet.PUBLISH)
	pkt := _pkt.(*packet.Publish)

	// [MQTT-3.3.1-9]
	// [MQTT-3.3.1-3]
	pkt.Set(p.Topic(), p.Payload(), p.QoS(), false, false) // nolint: errcheck

	s.tx.queuePacket(pkt)

	return nil
}

// forward PUBLISH message to topics manager which takes care about subscribers
func (s *Type) publishToTopic(msg *packet.Publish) error {
	// [MQTT-3.3.1.3]
	if msg.Retain() {
		if err := s.messenger.Retain(msg); err != nil {
			s.log.Error("Error retaining message", zap.String("ClientID", s.id), zap.Error(err))
		}

		// [MQTT-3.3.1-7]
		if msg.QoS() == packet.QoS0 {
			_m, _ := packet.NewMessage(s.version, packet.PUBLISH)
			m := _m.(*packet.Publish)
			m.SetQoS(msg.QoS())     // nolint: errcheck
			m.SetTopic(msg.Topic()) // nolint: errcheck
			s.retained.lock.Lock()
			s.retained.list = append(s.retained.list, m)
			s.retained.lock.Unlock()
		}
	}

	if err := s.messenger.Publish(msg); err != nil {
		s.log.Error("Couldn't publish", zap.String("ClientID", s.id), zap.Error(err))
	}

	return nil
}

// onReleaseIn ack process for incoming messages
func (s *Type) onReleaseIn(msg packet.Provider) {
	switch m := msg.(type) {
	case *packet.Publish:
		s.publishToTopic(m) // nolint: errcheck
	}
}

// onReleaseOut process messages that required ack cycle
// onAckTimeout if publish message has not been acknowledged withing specified ackTimeout
// server should mark it as a dup and send again
func (s *Type) onReleaseOut(msg packet.Provider) {
	switch msg.Type() {
	case packet.PUBACK:
		fallthrough
	case packet.PUBCOMP:
		id, _ := msg.ID()
		s.flowControl.release(id)
	}
}
