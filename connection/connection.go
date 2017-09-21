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

package connection

import (
	"container/list"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/persistence"
	"github.com/VolantMQ/volantmq/subscriber"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/types"
	"github.com/troian/easygo/netpoll"
	"go.uber.org/zap"
)

// nolint: golint
var (
	ErrOverflow    = errors.New("session: overflow")
	ErrPersistence = errors.New("session: error during persistence restore")
)

// DisconnectParams session state when stopped
type DisconnectParams struct {
	ExpireAt *uint32
	Desc     *netpoll.Desc
	Reason   packet.ReasonCode
	Will     bool
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

// PreConfig used by session manager to when configuring session
// a bit of ugly
// TODO(troian): try rid it off
type PreConfig struct {
	StartReceiving  func(*netpoll.Desc, netpoll.CallbackFn) error
	StopReceiving   func(*netpoll.Desc) error
	WaitForData     func(*netpoll.Desc) error
	Username        string
	AuthMethod      string
	AuthData        []byte
	State           persistence.Packets
	Metric          systree.Metric
	Conn            net.Conn
	Auth            auth.SessionPermissions
	Desc            *netpoll.Desc
	MaxRxPacketSize uint32
	MaxTxPacketSize uint32
	SendQuota       int32
	MaxTxTopicAlias uint16
	MaxRxTopicAlias uint16
	KeepAlive       uint16
	Version         packet.ProtocolVersion
	RetainAvailable bool
	PreserveOrder   bool
	OfflineQoS0     bool
}

// Config is system wide configuration parameters for every session
type Config struct {
	*PreConfig
	ID               string
	Subscriber       subscriber.ConnectionProvider
	Messenger        types.TopicMessenger
	OnDisconnect     onDisconnect
	ExpireIn         *uint32
	WillDelay        uint32
	KillOnDisconnect bool
}

// Type connection
type Type struct {
	*Config
	pubIn            *ackQueue
	pubOut           *ackQueue
	flowControl      *packetsFlowControl
	tx               *transmitter
	rx               *receiver
	onDisconnect     onDisconnect
	quit             chan struct{}
	onStart          types.Once
	onConnDisconnect types.Once
	started          sync.WaitGroup
	topicAlias       map[uint16]string
	retained         struct {
		lock sync.Mutex
		list []*packet.Publish
	}

	preProcessPublish  func(*packet.Publish) error
	postProcessPublish func(*packet.Publish) error

	log  *zap.Logger
	will bool
}

type unacknowledged struct {
	packet packet.Provider
}

func (u *unacknowledged) Size() (int, error) {
	return u.packet.Size()
}

type sizeAble interface {
	Size() (int, error)
}

// New allocate new connection object
func New(c *Config) (s *Type, err error) {
	s = &Type{
		Config:       c,
		onDisconnect: c.OnDisconnect,
		quit:         make(chan struct{}),
		topicAlias:   make(map[uint16]string),
		will:         true,
	}

	s.started.Add(1)
	s.pubIn = newAckQueue(s.onReleaseIn)
	s.pubOut = newAckQueue(s.onReleaseOut)
	s.flowControl = newFlowControl(s.quit, c.SendQuota)

	s.log = configuration.GetLogger().Named("connection." + s.ID)

	if s.Version >= packet.ProtocolV50 {
		s.preProcessPublish = s.preProcessPublishV50
		s.postProcessPublish = s.postProcessPublishV50
	} else {
		s.preProcessPublish = func(*packet.Publish) error { return nil }
		s.postProcessPublish = func(*packet.Publish) error { return nil }
	}

	s.tx = newTransmitter(&transmitterConfig{
		quit:          s.quit,
		log:           s.log,
		id:            s.ID,
		pubIn:         s.pubIn,
		pubOut:        s.pubOut,
		flowControl:   s.flowControl,
		conn:          s.Conn,
		onDisconnect:  s.onConnectionClose,
		maxPacketSize: c.MaxTxPacketSize,
		topicAliasMax: c.MaxTxTopicAlias,
	})

	var keepAlive time.Duration
	if c.KeepAlive > 0 {
		keepAlive = time.Second * time.Duration(c.KeepAlive)
		keepAlive = keepAlive + (keepAlive / 2)
	}

	s.rx = newReceiver(&receiverConfig{
		keepAlive:     keepAlive,
		quit:          s.quit,
		conn:          s.Conn,
		waitForRead:   s.onWaitForRead,
		onDisconnect:  s.onConnectionClose,
		onPacket:      s.processIncoming,
		version:       s.Version,
		maxPacketSize: c.MaxRxPacketSize,
		will:          &s.will,
	})

	gList := list.New()
	qList := list.New()

	subscribedPublish := func(p *packet.Publish) {
		if p.QoS() == packet.QoS0 {
			gList.PushBack(p)
		} else {
			qList.PushBack(p)
		}
	}

	// transmitter queue is ready, assign to subscriber new online callback
	// and signal it to forward messages to online callback by creating channel
	s.Subscriber.Online(subscribedPublish)

	// restore persisted state of the session if any
	if err = s.loadPersistence(); err != nil {
		return
	}

	s.Subscriber.OnlineRedirect(s.onSubscribedPublish)

	s.tx.gLoadList(gList)
	s.tx.qLoadList(qList)

	return
}

// Start run connection
func (s *Type) Start() {
	s.onStart.Do(func() {
		s.tx.run()
		s.StartReceiving(s.Desc, s.rx.run) // nolint: errcheck
		s.started.Done()
	})
}

// Stop session. Function assumed to be invoked once server about to either shutdown, disconnect
// or session is being replaced
// Effective only first invoke
func (s *Type) Stop(reason packet.ReasonCode) {
	s.onConnectionClose(true, reason)
}

func (s *Type) processIncoming(p packet.Provider) error {
	var err error
	var resp packet.Provider

	switch pkt := p.(type) {
	case *packet.Publish:
		resp, err = s.onPublish(pkt)
	case *packet.Ack:
		resp = s.onAck(pkt)
	case *packet.Subscribe:
		resp = s.onSubscribe(pkt)
	case *packet.UnSubscribe:
		resp = s.onUnSubscribe(pkt)
	case *packet.PingReq:
		// For PINGREQ message, we should send back PINGRESP
		mR, _ := packet.New(s.Version, packet.PINGRESP)
		resp, _ = mR.(*packet.PingResp)
	case *packet.Disconnect:
		// For DISCONNECT message, we should quit without sending Will
		s.will = false

		if s.Version == packet.ProtocolV50 {
			// FIXME: CodeRefusedBadUsernameOrPassword has same id as CodeDisconnectWithWill
			if pkt.ReasonCode() == packet.CodeRefusedBadUsernameOrPassword {
				s.will = true
			}

			err = errors.New("disconnect")

			if prop := pkt.PropertyGet(packet.PropertySessionExpiryInterval); prop != nil {
				if val, ok := prop.AsInt(); ok == nil {
					// If the Session Expiry Interval in the CONNECT packet was zero, then it is a Protocol Error to set a non-
					// zero Session Expiry Interval in the DISCONNECT packet sent by the Client. If such a non-zero Session
					// Expiry Interval is received by the Server, it does not treat it as a valid DISCONNECT packet. The Server
					// uses DISCONNECT with Reason Code 0x82 (Protocol Error) as described in section 4.13.
					if (s.ExpireIn != nil && *s.ExpireIn == 0) && val != 0 {
						err = packet.CodeProtocolError
					} else {
						s.ExpireIn = &val
					}
				}
			}
		}
	default:
		s.log.Error("Unsupported incoming message type",
			zap.String("ClientID", s.ID),
			zap.String("type", p.Type().Name()))
		return nil
	}

	if resp != nil {
		s.tx.gPush(resp)
	}

	return err
}

func (s *Type) loadPersistence() error {
	if s.State == nil {
		return nil
	}

	return s.State.PacketsForEach([]byte(s.ID), func(entry persistence.PersistedPacket) error {
		var err error
		var pkt packet.Provider
		if pkt, _, err = packet.Decode(s.Version, entry.Data); err != nil {
			s.log.Error("Couldn't decode persisted message", zap.Error(err))
			return ErrPersistence
		}

		if entry.UnAck {
			switch p := pkt.(type) {
			case *packet.Publish:
				id, _ := p.ID()
				s.flowControl.reAcquire(id)
			case *packet.Ack:
				id, _ := p.ID()
				s.flowControl.reAcquire(id)
			}

			s.tx.qLoad(&unacknowledged{packet: pkt})
		} else {
			if p, ok := pkt.(*packet.Publish); ok {
				if len(entry.ExpireAt) > 0 {
					if tm, err := time.Parse(time.RFC3339, entry.ExpireAt); err == nil {
						p.SetExpiry(tm)
					} else {
						s.log.Error("Parse publish expiry", zap.String("ClientID", s.ID), zap.Error(err))
					}
				}

				if p.QoS() == packet.QoS0 {
					s.tx.gLoad(pkt)
				} else {
					s.tx.qLoad(pkt)
				}
			}
		}

		return nil
	})
}

func (s *Type) persist() {
	var packets []persistence.PersistedPacket

	persistAppend := func(p interface{}) {
		var pkt packet.Provider
		pPkt := persistence.PersistedPacket{UnAck: false}

		switch tp := p.(type) {
		case *packet.Publish:
			if (s.OfflineQoS0 || tp.QoS() != packet.QoS0) && !tp.Expired(false) {
				if tm := tp.GetExpiry(); !tm.IsZero() {
					pPkt.ExpireAt = tm.Format(time.RFC3339)
				}

				if tp.QoS() != packet.QoS0 {
					// make sure message has some IDType to prevent encode error
					tp.SetPacketID(0)
				}

				pkt = tp
			}
		case *unacknowledged:
			if pb, ok := tp.packet.(*packet.Publish); ok && pb.QoS() == packet.QoS1 {
				pb.SetDup(true)
			}

			pkt = tp.packet
			pPkt.UnAck = true
		}

		var err error
		if pPkt.Data, err = packet.Encode(pkt); err != nil {
			s.log.Error("Couldn't encode message for persistence", zap.Error(err))
		} else {
			packets = append(packets, pPkt)
		}
	}

	var next *list.Element
	for elem := s.tx.qMessages.Front(); elem != nil; elem = next {
		next = elem.Next()

		persistAppend(s.tx.qMessages.Remove(elem))
	}

	for elem := s.tx.gMessages.Front(); elem != nil; elem = next {
		next = elem.Next()
		switch tp := s.tx.gMessages.Remove(elem).(type) {
		case *packet.Publish:
			persistAppend(tp)
		}
	}

	s.pubOut.messages.Range(func(k, v interface{}) bool {
		if pkt, ok := v.(packet.Provider); ok {
			persistAppend(&unacknowledged{packet: pkt})
		}
		return true
	})

	if err := s.State.PacketsStore([]byte(s.ID), packets); err != nil {
		s.log.Error("Persist packets", zap.String("ClientID", s.ID), zap.Error(err))
	}
}

// onSubscribedPublish is the method that gets added to the topic subscribers list by the
// processSubscribe() method. When the server finishes the ack cycle for a
// PUBLISH message, it will call the subscriber, which is this method.
//
// For the server, when this method is called, it means there's a message that
// should be published to the client on the other end of this connection. So we
// will call publish() to send the message.
func (s *Type) onSubscribedPublish(p *packet.Publish) {
	if p.QoS() == packet.QoS0 {
		s.tx.gPush(p)
	} else {
		s.tx.qPush(p)
	}
}

// forward PUBLISH message to topics manager which takes care about subscribers
func (s *Type) publishToTopic(p *packet.Publish) error {
	if err := s.postProcessPublish(p); err != nil {
		return err
	}

	p.SetPublishID(s.Subscriber.Hash())

	// [MQTT-3.3.1.3]
	if p.Retain() {
		if err := s.Messenger.Retain(p); err != nil {
			s.log.Error("Error retaining message", zap.String("ClientID", s.ID), zap.Error(err))
		}

		// [MQTT-3.3.1-7]
		if p.QoS() == packet.QoS0 {
			_m, _ := packet.New(s.Version, packet.PUBLISH)
			m := _m.(*packet.Publish)
			m.SetQoS(p.QoS())     // nolint: errcheck
			m.SetTopic(p.Topic()) // nolint: errcheck
			s.retained.lock.Lock()
			s.retained.list = append(s.retained.list, m)
			s.retained.lock.Unlock()
		}
	}

	if err := s.Messenger.Publish(p); err != nil {
		s.log.Error("Couldn't publish", zap.String("ClientID", s.ID), zap.Error(err))
	}

	return nil
}

// onReleaseIn ack process for incoming messages
func (s *Type) onReleaseIn(o, n packet.Provider) {
	switch p := o.(type) {
	case *packet.Publish:
		s.publishToTopic(p) // nolint: errcheck
	}
}

// onReleaseOut process messages that required ack cycle
// onAckTimeout if publish message has not been acknowledged withing specified ackTimeout
// server should mark it as a dup and send again
func (s *Type) onReleaseOut(o, n packet.Provider) {
	switch n.Type() {
	case packet.PUBACK:
		fallthrough
	case packet.PUBCOMP:
		id, _ := n.ID()
		if s.flowControl.release(id) {
			s.tx.signalQuota()
		}
	}
}

func (s *Type) onWaitForRead() error {
	return s.WaitForData(s.Desc)
}

func (s *Type) preProcessPublishV50(p *packet.Publish) error {
	// v5.0
	// If the Server included Retain Available in its CONNACK response to a Client with its value set to 0 and it
	// receives a PUBLISH packet with the RETAIN flag is set to 1, then it uses the DISCONNECT Reason
	// Code of 0x9A (Retain not supported) as described in section 4.13.
	if s.Version >= packet.ProtocolV50 && !s.RetainAvailable && p.Retain() {
		return packet.CodeRetainNotSupported
	}

	if prop := p.PropertyGet(packet.PropertyTopicAlias); prop != nil {
		if val, ok := prop.AsShort(); ok == nil && (val == 0 || val > s.MaxRxTopicAlias) {
			return packet.CodeInvalidTopicAlias
		}
	}

	return nil
}

func (s *Type) postProcessPublishV50(p *packet.Publish) error {
	// [MQTT-3.3.2.3.4]
	if prop := p.PropertyGet(packet.PropertyTopicAlias); prop != nil {
		if val, err := prop.AsShort(); err == nil {
			if len(p.Topic()) != 0 {
				// renew alias with new topic
				s.topicAlias[val] = p.Topic()
			} else {
				if topic, kk := s.topicAlias[val]; kk {
					// do not check for error as topic has been validated when arrived
					p.SetTopic(topic) // nolint: errcheck
				} else {
					return packet.CodeInvalidTopicAlias
				}
			}
		} else {
			return packet.CodeInvalidTopicAlias
		}
	}

	// [MQTT-3.3.2.3.3]
	if prop := p.PropertyGet(packet.PropertyPublicationExpiry); prop != nil {
		if val, err := prop.AsInt(); err == nil {
			p.SetExpiry(time.Now().Add(time.Duration(val) * time.Second))
		} else {
			return err
		}
	}

	return nil
}
