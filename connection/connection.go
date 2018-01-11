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

	"github.com/VolantMQ/persistence"
	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/packet"
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
	EventPoll        netpoll.EventPoll
	Username         string
	AuthMethod       string
	AuthData         []byte
	PersistedSession persistence.Sessions
	WasPersisted     bool
	Metric           systree.Metric
	Conn             net.Conn
	Auth             auth.SessionPermissions
	Desc             *netpoll.Desc
	MaxRxPacketSize  uint32
	MaxTxPacketSize  uint32
	SendQuota        int32
	MaxTxTopicAlias  uint16
	MaxRxTopicAlias  uint16
	KeepAlive        uint16
	Version          packet.ProtocolVersion
	RetainAvailable  bool
	PreserveOrder    bool
	OfflineQoS0      bool
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
	preProcessPublish  func(*packet.Publish) error
	postProcessPublish func(*packet.Publish) error
	pubIn              ackQueue
	pubOut             ackQueue
	quit               chan struct{}
	onStart            types.Once
	onConnDisconnect   types.Once
	started            sync.WaitGroup
	txWg               sync.WaitGroup
	rxWg               sync.WaitGroup
	txTopicAlias       map[string]uint16
	rxTopicAlias       map[uint16]string
	txTimer            *time.Timer
	log                *zap.Logger
	keepAliveTimer     *time.Timer
	txGMessages        list.List
	txQMessages        list.List
	txGLock            sync.Mutex
	txQLock            sync.Mutex
	keepAlive          time.Duration
	txAvailable        chan int
	rxRecv             []byte
	retained           struct {
		lock sync.Mutex
		list []*packet.Publish
	}
	flowInUse         sync.Map
	flowCounter       uint64
	rxRemaining       int
	txRunning         uint32
	rxRunning         uint32
	topicAliasCurrMax uint16
	txQuotaExceeded   bool
	will              bool
	// the unique message IDs(createdAt field of the PUBLISH packet) of the
	// lasted completed message(PUBLISH packet with QoS1 or QoS2) by the remote subscribers.
	// map[topic name]map[message createdAt]true
	lastCompletedSubscribedMessageUniqueIds *LastCompletedSubscribedMessageUniqueIds
}

type LastCompletedSubscribedMessageUniqueIds struct{
	lock sync.RWMutex
	// the subscribed topics and the last retained message IDs(createdAt field of the PUBLISH packet)
	topics *map[string]int64
}

func (lids *LastCompletedSubscribedMessageUniqueIds)Store(p *packet.Publish){
	lids.lock.Lock()
	topic := p.Topic()
	(*lids.topics)[topic] = p.GetCreateTimestamp()
	lids.lock.Unlock()
}

func(lids *LastCompletedSubscribedMessageUniqueIds)Exists(p *packet.Publish)bool{
	lids.lock.RLock()
	defer lids.lock.RUnlock()
	if id, ok := (*lids.topics)[p.Topic()]; ok && p.GetCreateTimestamp() == id {
		return true
	}
	return false
}

func(lids *LastCompletedSubscribedMessageUniqueIds) TopicMessageIDs()*map[string]int64{
	return lids.topics
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
		quit:         make(chan struct{}),
		txAvailable:  make(chan int, 1),
		txTopicAlias: make(map[string]uint16),
		rxTopicAlias: make(map[uint16]string),
		txTimer:      time.NewTimer(1 * time.Second),
		will:         true,
		lastCompletedSubscribedMessageUniqueIds: &LastCompletedSubscribedMessageUniqueIds{
			topics:&map[string]int64{},
		},
	}

	s.txTimer.Stop()

	s.started.Add(1)
	s.pubIn.onRelease = s.onReleaseIn
	s.pubOut.onRelease = s.onReleaseOut

	s.log = configuration.GetLogger().Named("connection." + s.ID)

	if s.Version >= packet.ProtocolV50 {
		s.preProcessPublish = s.preProcessPublishV50
		s.postProcessPublish = s.postProcessPublishV50
	} else {
		s.preProcessPublish = func(*packet.Publish) error { return nil }
		s.postProcessPublish = func(*packet.Publish) error { return nil }
	}

	if c.KeepAlive > 0 {
		s.keepAlive = time.Second * time.Duration(c.KeepAlive)
		s.keepAlive = s.keepAlive + (s.keepAlive / 2)
		s.keepAliveTimer = time.AfterFunc(s.keepAlive, s.keepAliveExpired)
	}

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

	s.gLoadList(gList)
	s.qLoadList(qList)

	return
}

// Start run connection
func (s *Type) Start() {
	s.onStart.Do(func() {
		s.txRun()
		err := s.EventPoll.Start(s.Desc, s.rxRun) // nolint: errcheck
		if err != nil {
			s.log.Error("Failed to monitor client connection.", zap.String("err", err.Error()))
		}
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
		err = errors.New("disconnect")
		if s.Version == packet.ProtocolV50 {
			// FIXME: CodeRefusedBadUsernameOrPassword has same id as CodeDisconnectWithWill
			if pkt.ReasonCode() == packet.CodeRefusedBadUsernameOrPassword {
				s.will = true
			}

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
		s.log.Debug("Client requested to disconnect, session is going to be shutdown.",
			zap.String("client_id", s.ID), zap.String("client_addr", s.Conn.RemoteAddr().String()))
	default:
		s.log.Error("Unsupported incoming message type on flight stage",
			zap.String("ClientID", s.ID),
			zap.String("type", p.Type().Name()))
		return nil
	}

	if resp != nil {
		s.gPush(resp)
	}

	return err
}

func (s *Type) loadPersistence() error {
	if s.PersistedSession == nil {
		return nil
	}
	var err error
	err = s.PersistedSession.PacketsForEach([]byte(s.ID), func(entry persistence.PersistedPacket) error {
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
				s.flowReAcquire(id)
			case *packet.Ack:
				id, _ := p.ID()
				s.flowReAcquire(id)
			}

			s.qLoad(&unacknowledged{packet: pkt})
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
					s.gLoad(pkt)
				} else {
					s.qLoad(pkt)
				}
			}
		}

		return nil
	})

	if err != nil {
		return err
	} else {
		err = s.PersistedSession.PacketsDelete([]byte(s.ID))
		if err != nil && err != persistence.ErrNotFound {
			return err
		} else if err == nil{
			s.log.Debug("Persisted packets are loaded and cleaned on persistent.", zap.String("ClientID", s.ID))
		} else {
			s.log.Debug("No persisted packets were found for client .", zap.String("ClientID", s.ID))
		}
	}


	// load session state.
	if state, _:= s.PersistedSession.StateGet([]byte(s.ID)); state != nil {
		s.WasPersisted = true
		s.SetLastCompletedSubscribedMessageUniqueTopicIds(state.LastCompletedSubscribedMessageUniqueIds)
		s.log.Debug("Persisted session state loaded:", zap.String("ClientID", s.ID), zap.Any("state", *state))
	} else {
		s.log.Debug("No persisted session state found:", zap.String("ClientID", s.ID))
	}
	return nil
}

func (s *Type) persist() {
	if s.PersistedSession == nil {
		return
	}
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
	for elem := s.txQMessages.Front(); elem != nil; elem = next {
		next = elem.Next()

		persistAppend(s.txQMessages.Remove(elem))
	}

	for elem := s.txGMessages.Front(); elem != nil; elem = next {
		next = elem.Next()
		switch tp := s.txGMessages.Remove(elem).(type) {
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

	if err := s.PersistedSession.PacketsStore([]byte(s.ID), packets); err != nil {
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
		s.gPush(p)
	} else {
		s.qPush(p)
	}
}

// forward PUBLISH message to topics manager which takes care about subscribers
func (s *Type) publishToTopic(p *packet.Publish) error {
	// TODO: right place?
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
		if s.flowRelease(id) {
			s.signalQuota()
		}
	}
	if p, ok := o.(*packet.Publish); ok {
		s.StoreLastCompletedSubscribedMessageUniqueId(p)
	}
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
				s.rxTopicAlias[val] = p.Topic()
			} else {
				if topic, kk := s.rxTopicAlias[val]; kk {
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

func (s *Type) StoreLastCompletedSubscribedMessageUniqueId(p *packet.Publish){
	s.lastCompletedSubscribedMessageUniqueIds.Store(p)
}

// SubscribedMessageCompleted checks it the PUBLISH packet has been completed by the remove subscriber ever before.
// this is to insure QoS1 or QoS 2
func (s *Type) SubscribedMessageCompleted(p *packet.Publish)bool{
	return s.lastCompletedSubscribedMessageUniqueIds.Exists(p)
}

func (s *Type) GetLastCompletedSubscribedMessageUniqueTopicIds()*map[string]int64{
	return s.lastCompletedSubscribedMessageUniqueIds.topics
}

func (s *Type) SetLastCompletedSubscribedMessageUniqueTopicIds(topicIds *map[string]int64){
	s.lastCompletedSubscribedMessageUniqueIds.topics = topicIds
}