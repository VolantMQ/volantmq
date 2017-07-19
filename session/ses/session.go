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

package session

import (
	"sync"

	"container/list"

	"io"

	"errors"

	"sync/atomic"

	"github.com/troian/surgemq/configuration"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/session/connection"
	"github.com/troian/surgemq/subscriber"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics/types"
	"go.uber.org/zap"
)

var (
	ErrOverflow    = errors.New("overflow")
	ErrPersistence = errors.New("error during persistence restore")
)

type Callbacks struct {
	// OnClose called when session has done all work and should be deleted
	//OnStop func(id string, s message.TopicQos)
	// OnDisconnect called when session stopped net connection and should be either suspended or deleted
	OnStop func(id string, state *persistenceTypes.SessionState)
	// OnPublish
	OnPublish func(id string, msg *message.PublishMessage)
}

type WillConfig struct {
	Topic   string
	Message []byte
	Retain  bool
	QoS     message.QosType
}

// Config is system wide configuration parameters for every session
type Config struct {
	Id string

	State      persistenceTypes.Session
	Subscriber subscriber.SessionProvider
	Messenger  topicsTypes.Messenger
	Conn       io.Closer

	Callbacks *Callbacks
	Will      *message.PublishMessage

	Metric struct {
		Packets systree.PacketsMetric
		Session systree.SessionStat
	}

	KeepAlive   int
	SendQuota   int32
	OfflineQoS0 bool
	Clean       bool
	Version     message.ProtocolVersion
}

// Type session
type Type struct {
	packetID  uint64
	callbacks *Callbacks
	will      *message.PublishMessage
	conn      *connection.Provider
	pubIn     *ackQueue
	pubOut    *ackQueue

	subscriber subscriber.SessionProvider
	messenger  topicsTypes.Messenger

	publisher publisher

	id string

	onStart sync.Once
	onStop  sync.Once
	started sync.WaitGroup

	packetIds struct {
		inUse map[message.PacketID]bool
		mu    sync.Mutex
	}

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

// New allocate new sessions object
func New(config *Config) (s *Type, present bool, err error) {
	defer func() {
		if err != nil {
			config.Subscriber.PutOffline(true)
			s = nil
		}
	}()

	present = false

	s = &Type{
		id:          config.Id,
		subscriber:  config.Subscriber,
		messenger:   config.Messenger,
		version:     config.Version,
		clean:       config.Clean,
		offlineQoS0: config.OfflineQoS0,
		sendQuota:   config.SendQuota,
		callbacks:   config.Callbacks,
		will:        config.Will,
		publisher: publisher{
			messages: list.New(),
		},
	}

	s.started.Add(1)

	s.packetIds.inUse = make(map[message.PacketID]bool)

	s.log.prod = configuration.GetProdLogger().Named("session." + s.id)
	s.log.dev = configuration.GetDevLogger().Named("session." + s.id)

	s.publisher.cond = sync.NewCond(&s.publisher.lock)

	// publisher queue is ready, assign to subscriber new online callback
	// and signal it to forward messages to online callback by creating channel
	s.subscriber.SetOnlineCallback(s.onSubscribedPublish)

	s.pubIn = newAckQueue(s.onReleaseIn)
	s.pubOut = newAckQueue(s.onReleaseOut)

	// now is time to restore persisted state of the session if any
	if !s.clean && config.State != nil {
		var state *persistenceTypes.SessionState
		if state, err = config.State.Get([]byte(s.id)); err != nil && err != persistenceTypes.ErrNotFound {
			return
		}

		if state != nil {
			for _, d := range state.UnAckMessages {
				var msg message.Provider
				if msg, _, err = message.Decode(s.version, d); err != nil {
					s.log.prod.Error("Couldn't decode persisted message", zap.Error(err))
					err = ErrPersistence
					return
				}

				switch m := msg.(type) {
				case *message.PublishMessage:
					if m.QoS() == message.QoS2 && m.Dup() {
						s.log.prod.Error("2: QoS2 DUP")
					}
				}

				s.pubOut.store(msg)
				id, _ := msg.PacketID()
				s.packetIds.inUse[id] = true
			}

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

				s.publisher.messages.PushBack(msg)
			}

			present = true
		}
	}

	s.publisher.quit = make(chan struct{})

	// Run connection
	// error check check at deferred call
	s.conn, err = connection.New(
		&connection.Config{
			ID:           s.id,
			Conn:         config.Conn,
			KeepAlive:    config.KeepAlive,
			ProtoVersion: config.Version,
			On: connection.OnProcess{
				Publish:     s.onPublish,
				Ack:         s.onAck,
				Subscribe:   s.onSubscribe,
				UnSubscribe: s.onUnSubscribe,
				Disconnect:  s.onConnectionClose,
			},
			PacketsMetric: config.Metric.Packets,
		})
	return
}

// Start signal session to start processing messages.
// Effective only first invoke
func (s *Type) Start() {
	s.onStart.Do(func() {
		defer s.started.Done()

		// make copy of messages requires acknowledgment and send them from this copy
		// because of ack.get() is not thread-safe and remote might send responses during these writes
		tmpAcks := map[message.PacketID]message.Provider{}

		for k, v := range s.pubOut.get() {
			tmpAcks[k] = v
		}

		s.conn.Start()

		// deliver persisted acks if any
		for _, m := range tmpAcks {
			if _, err := s.conn.WriteMessage(m); err != nil {
				s.log.prod.Error("Couldn't redeliver ack", zap.String("ClientID", s.id), zap.Error(err))
			}
		}

		s.publisher.stopped.Add(1)
		s.publisher.started.Add(1)
		go s.publishWorker()
		s.publisher.started.Wait()
	})
}

// Stop session. Function assumed to be invoked once server about to shutdown or session is being replaced
// Effective only first invoke
func (s *Type) Stop() {
	s.onStop.Do(func() {
		s.conn.Stop()
	})
}

// onSubscribedPublish is the method that gets added to the topic subscribers list by the
// processSubscribe() method. When the server finishes the ack cycle for a
// PUBLISH message, it will call the subscriber, which is this method.
//
// For the server, when this method is called, it meansƒ there's a message that
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

	s.publisher.lock.Lock()
	s.publisher.messages.PushBack(m)
	s.publisher.lock.Unlock()
	s.publisher.cond.Signal()

	return nil
}

func (s *Type) acquirePacketID() (message.PacketID, error) {
	defer s.packetIds.mu.Unlock()

	s.packetIds.mu.Lock()
	if len(s.packetIds.inUse) >= int(^uint16(0)>>1) {
		return 0, ErrOverflow
	}

	var id message.PacketID
	found := false
	for count := 0; count < int(^uint16(0)>>1); count++ {
		s.packetID++
		id = message.PacketID(s.packetID)

		if _, ok := s.packetIds.inUse[id]; !ok {
			found = true
			s.packetIds.inUse[id] = true
			break
		}
	}

	if !found {
		return 0, ErrOverflow
	}

	return id, nil
}

func (s *Type) releasePacketID(id message.PacketID) {
	defer s.packetIds.mu.Unlock()
	s.packetIds.mu.Lock()

	delete(s.packetIds.inUse, id)
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

	msg.SetRetain(false)

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
		// TODO: release packet ID
		id, _ := msg.PacketID()
		s.releasePacketID(id)

		atomic.AddInt32(&s.sendQuota, 1)
	}
}

// publishWorker publish messages coming from subscribed topics
func (s *Type) publishWorker() {
	defer func() {
		s.publisher.stopped.Done()

		s.Stop()
	}()

	s.publisher.started.Done()

	for {
		if s.publisher.isDone() {
			return
		}

		s.publisher.cond.L.Lock()
		for s.publisher.messages.Len() == 0 {
			s.publisher.cond.Wait()
			if s.publisher.isDone() {
				s.publisher.cond.L.Unlock()
				return
			}
		}

		var msg *message.PublishMessage

		elem := s.publisher.messages.Front()
		msg = elem.Value.(*message.PublishMessage)
		s.publisher.messages.Remove(elem)

		// V5.0 [MQTT-4.9.0-2]
		if s.version == message.ProtocolV50 && atomic.LoadInt32(&s.sendQuota) == 0 && msg.QoS() > message.QoS0 {
			s.publisher.messages.PushBack(msg)
			msg = nil
		}

		s.publisher.cond.L.Unlock()

		if msg != nil {
			var id message.PacketID
			var err error
			if msg.QoS() != message.QoS0 {
				if id, err = s.acquirePacketID(); err == nil {
					msg.SetPacketID(id)
				} else {
					s.log.prod.Error("Session has too many unacknowledged packets. Disconnecting")
					return
				}
			}

			if msg.QoS() == message.QoS2 && msg.Dup() {
				s.log.prod.Error("1: QoS2 DUP")
			}

			if _, err = s.conn.WriteMessage(msg); err == nil {
				if msg.QoS() != message.QoS0 {
					s.pubOut.store(msg)
				}
			} else {
				// Couldn't deliver message to client thus requeue it back
				// Error during write means connection has been closed/timedout etc
				// thus we do not requeue message of QoS0
				if msg.QoS() != message.QoS0 {
					s.releasePacketID(id)
					s.publisher.cond.L.Lock()
					s.publisher.messages.PushBack(msg)
					s.publisher.cond.L.Unlock()
				}
			}
		}
	}
}
