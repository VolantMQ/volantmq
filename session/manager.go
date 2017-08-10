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
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"sync"

	"encoding/binary"

	"unsafe"

	"time"

	"github.com/troian/surgemq/configuration"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/routines"
	"github.com/troian/surgemq/session/ses"
	"github.com/troian/surgemq/subscriber"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics/types"
	"go.uber.org/zap"
)

var (
	// ErrNotAccepted new connection does not meet requirements
	ErrNotAccepted = errors.New("Connection not accepted")

	// ErrDupNotAllowed case when new client with existing ID connected
	ErrDupNotAllowed = errors.New("duplicate not allowed")
)

type clientConnectStatus struct {
	Address        string
	Username       string
	CleanSession   bool
	SessionPresent bool
	Protocol       message.ProtocolVersion
	Timestamp      string
	ConnAckCode    message.ReasonCode
}

type clientDisconnectStatus struct {
	Reason    string
	Timestamp string
}

// Config manager configuration
type Config struct {
	// Topics manager for all the client subscriptions
	TopicsMgr topicsTypes.Provider

	NodeName string

	// The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// The number of seconds to keep the connection live if there's no data.
	// If not set then defaults to 5 minutes.
	KeepAlive int

	Systree systree.Provider

	// OnDuplicate If requested we notify if there is attempt to dup session
	OnDuplicate func(string, bool)

	// AllowDuplicates Either allow or deny replacing of existing session if there new client
	// with same clientID
	AllowDuplicates bool

	Persist persistenceTypes.Provider

	OfflineQoS0 bool
}

type activeSessions struct {
	list  map[string]*session.Type
	lock  sync.RWMutex
	count sync.WaitGroup
}

type subscribedTopicConfig struct {
	ops message.SubscriptionOptions
	id  uint32
}

// Manager interface
type Manager struct {
	config Config

	sessions activeSessions

	subsLock    sync.Mutex
	subscribers map[string]subscriber.Provider

	lock sync.Mutex
	quit chan struct{}

	pSessions persistenceTypes.Sessions

	log struct {
		prod *zap.Logger
		dev  *zap.Logger
	}
}

// New alloc new
func New(cfg Config) (*Manager, error) {
	//if config.Stat == nil {
	//	return nil, errors.New("No stat provider")
	//}

	if cfg.Persist == nil {
		return nil, errors.New("No persist provider")
	}

	m := &Manager{
		config: cfg,
		quit:   make(chan struct{}),
	}

	m.log.prod = configuration.GetProdLogger().Named("manager.session")
	m.log.dev = configuration.GetDevLogger().Named("manager.session")

	m.sessions.list = make(map[string]*session.Type)
	m.subscribers = make(map[string]subscriber.Provider)

	var err error
	if m.pSessions, err = m.config.Persist.Sessions(); err != nil {
		return nil, err
	}

	// load sessions owning subscriptions
	if sHandler, err := m.config.Persist.Subscriptions(); err == nil {
		type subscriberConfig struct {
			version message.ProtocolVersion
			topics  subscriber.Subscriptions
		}
		subs := map[string]subscriberConfig{}

		err = sHandler.Load(func(id []byte, data []byte) error {
			sub := subscriber.Subscriptions{}
			offset := 0
			version := message.ProtocolVersion(data[offset])
			offset++
			remaining := len(data) - 1
			for offset != remaining {
				t, total, e := message.ReadLPBytes(data[offset:])
				if e != nil {
					return err
				}

				offset += total

				params := &subscriber.SubscriptionParams{}

				params.Requested = message.SubscriptionOptions(data[offset])
				offset++

				params.ID = binary.BigEndian.Uint32(data[offset:])
				offset += 4
				sub[string(t)] = params
			}

			subs[string(id)] = subscriberConfig{
				version: version,
				topics:  sub,
			}

			return nil
		})

		if err != nil && err != persistenceTypes.ErrNotFound {
			return nil, err
		}

		for id, t := range subs {
			sub := subscriber.New(&subscriber.Config{
				ID:          id,
				Offline:     m.onPublish,
				Topics:      m.config.TopicsMgr,
				Version:     t.version,
				OfflineQoS0: m.config.OfflineQoS0,
			})
			for topic, ops := range t.topics {
				if _, _, err = sub.Subscribe(topic, ops); err != nil {
					m.log.prod.Error("Couldn't subscribe", zap.Error(err))
				}
			}

			m.subsLock.Lock()
			m.subscribers[id] = sub
			m.subsLock.Unlock()
			m.config.Systree.Sessions().Created()
		}
	}

	return m, nil
}

// Start try start new session
func (m *Manager) Start(msg *message.ConnectMessage, resp *message.ConnAckMessage, conn net.Conn) {
	var err error
	var ses *session.Type
	var id string
	present := false
	idGenerated := false
	allocError := false

	defer func() {
		if allocError {
			var reason message.ReasonCode
			switch msg.Version() {
			case message.ProtocolV50:
				reason = message.CodeUnspecifiedError
			default:
				reason = message.CodeRefusedServerUnavailable
			}
			resp.SetReturnCode(reason) // nolint: errcheck
		} else {
			resp.SetSessionPresent(present)

			if idGenerated {
				msg.PropertySet(message.PropertyAssignedClientIdentifier, id)
			}
		}
		m.log.prod.Info("Client connect",
			zap.String("Status", resp.ReturnCode().Desc()),
			zap.String("ClientID", id),
			zap.Bool("id generated", idGenerated),
			zap.Bool("Clean start", msg.CleanStart()),
			zap.Bool("Session present", present),
			zap.String("RemoteAddr", conn.(net.Conn).RemoteAddr().String()),
		)

		if err = routines.WriteMessage(conn, resp); err != nil {
			m.log.prod.Error("Couldn't write CONNACK", zap.String("ClientID", id), zap.Error(err))
		} else {
			if ses != nil {
				ses.Start()

				notifyPayload := systree.ClientConnectStatus{
					Address:        conn.RemoteAddr().String(),
					CleanSession:   msg.CleanStart(),
					SessionPresent: present,
					Protocol:       msg.Version(),
					Timestamp:      time.Now().Format(time.RFC3339),
					ConnAckCode:    resp.ReturnCode(),
				}

				m.config.Systree.Clients().Connected(id, &notifyPayload)
			}
		}
	}()

	select {
	case <-m.quit:
		var reason message.ReasonCode
		switch msg.Version() {
		case message.ProtocolV50:
			reason = message.CodeServerShuttingDown
		default:
			reason = message.CodeRefusedServerUnavailable
		}
		resp.SetReturnCode(reason) // nolint: errcheck
		return
	default:
	}

	if resp.ReturnCode() != message.CodeSuccess {
		return
	}

	id = string(msg.ClientID())
	if len(id) == 0 {
		id = m.genSessionID()
		idGenerated = true
	}

	defer m.sessions.lock.Unlock()
	m.sessions.lock.Lock()

	// Handle duplicate case
	if ses = m.sessions.list[id]; ses != nil {
		replace := false
		if !m.config.AllowDuplicates {
			// duplicate prohibited. send identifier rejected
			var reason message.ReasonCode
			switch msg.Version() {
			case message.ProtocolV50:
				reason = message.CodeInvalidClientID
			default:
				reason = message.CodeRefusedIdentifierRejected
			}

			resp.SetReturnCode(reason) // nolint: errcheck
			err = ErrDupNotAllowed
		} else {
			// duplicate allowed stop current session
			ses.Stop()
			m.config.Systree.Sessions().Removed()
			delete(m.sessions.list, id)
			replace = true
		}

		ses = nil
		// notify subscriber about dup attempt
		if m.config.OnDuplicate != nil {
			m.config.OnDuplicate(id, replace)
		}

		if !m.config.AllowDuplicates {
			return
		}
	}

	pState, _ := m.config.Persist.Sessions()

	if msg.CleanStart() {
		// check if session was in non-clean state before
		// if subscriber exists proceed to unsubscribe and delete the object
		m.subsLock.Lock()
		if sub, ok := m.subscribers[id]; ok {
			sub.PutOffline(true)
			m.config.Systree.Sessions().Removed()
			delete(m.subscribers, id)
		}
		m.subsLock.Unlock()

		// delete persisted state as it might be created before
		if err = pState.Delete([]byte(id)); err != nil && err != persistenceTypes.ErrNotFound {
			m.log.prod.Info("Cannot delete persisted session", zap.String("ClientID", id), zap.Error(err))
		}
	}

	// allocate new subscriber if there is no existing subscriber
	m.subsLock.Lock()
	sub := m.subscribers[id]
	if sub == nil {
		sub = subscriber.New(&subscriber.Config{
			ID:          id,
			Offline:     m.onPublish,
			Topics:      m.config.TopicsMgr,
			Version:     msg.Version(),
			OfflineQoS0: m.config.OfflineQoS0,
		})
		m.subscribers[id] = sub
		m.config.Systree.Sessions().Created()
	}
	m.subsLock.Unlock()

	cfg := &session.Config{
		ID:          id,
		Subscriber:  sub,
		Messenger:   m.config.TopicsMgr,
		Clean:       msg.CleanStart(),
		Version:     msg.Version(),
		Conn:        conn,
		KeepAlive:   int(msg.KeepAlive()),
		State:       pState,
		OfflineQoS0: m.config.OfflineQoS0,
		Metric:      m.config.Systree.Metric(),
		Callbacks: &session.Callbacks{
			OnStop: m.onStop,
		},
	}

	if willTopic, willPayload, willQoS, willRetain, will := msg.Will(); will {
		willMsg, _ := message.NewMessage(msg.Version(), message.PUBLISH)
		cfg.Will = willMsg.(*message.PublishMessage)
		cfg.Will.SetQoS(willQoS)     // nolint: errcheck
		cfg.Will.SetTopic(willTopic) // nolint: errcheck
		cfg.Will.SetPayload(willPayload)
		cfg.Will.SetRetain(willRetain)
	}

	if ses, present, err = session.New(cfg); err != nil {
		if e, ok := err.(persistenceTypes.Errors); ok {
			m.log.prod.Error("Couldn't start session. Retry without persistence",
				zap.String("ClientID", id),
				zap.Error(e))

			sub.PutOffline(true)

			sub = subscriber.New(&subscriber.Config{
				ID:          id,
				Offline:     m.onPublish,
				Topics:      m.config.TopicsMgr,
				Version:     msg.Version(),
				OfflineQoS0: m.config.OfflineQoS0,
			})
			m.subsLock.Lock()
			m.subscribers[id] = sub
			m.subsLock.Unlock()

			cfg.Subscriber = sub
			cfg.State = nil

			ses, present, err = session.New(cfg)
		}
	}

	// delete persisted state as it might be created before
	if e := pState.Delete([]byte(id)); e != nil && e != persistenceTypes.ErrNotFound {
		m.log.prod.Info("Cannot delete persisted session", zap.String("ClientID", id), zap.Error(e))
	}

	if err == nil {
		m.sessions.count.Add(1)
		//m.sessions.lock.Lock()
		m.sessions.list[id] = ses
		//m.sessions.lock.Unlock()
	} else {
		allocError = true
	}
}

// Shutdown manager
func (m *Manager) Shutdown() error {
	defer m.lock.Unlock()
	m.lock.Lock()

	select {
	case <-m.quit:
		return errors.New("already stopped")
	default:
		close(m.quit)
	}

	// 1. Now signal all active sessions to finish
	m.sessions.lock.Lock()
	for _, s := range m.sessions.list {
		s.Stop()
	}
	m.sessions.lock.Unlock()

	// 2. Wait until all active sessions stopped
	m.sessions.count.Wait()

	// 3. wipe list
	m.sessions.list = make(map[string]*session.Type)

	if sHandler, err := m.config.Persist.Subscriptions(); err == nil {
		// 4. shutdown and persist subscriptions from non-clean session
		for id, s := range m.subscribers {
			s.PutOffline(true)

			topics := s.Subscriptions()

			// calculate size of the encoded entry
			// consist of:
			// 1 byte - protocol version followed by subscription each of them consist of:
			// 2 bytes - length prefix
			// n bytes - topic
			// 1 byte - topic options
			// 4 bytes - subscription id

			size := 0
			for topic := range topics {
				size += 2 + len(topic) + 1 + int(unsafe.Sizeof(uint32(0)))
			}

			buf := make([]byte, size+1)
			offset := 0
			buf[offset] = byte(s.Version())
			offset++

			for s, params := range topics {
				total, _ := message.WriteLPBytes(buf[offset:], []byte(s))
				offset += total
				buf[offset] = byte(params.Requested)
				offset++
				binary.BigEndian.PutUint32(buf[offset:], params.ID)
				offset += 4
			}

			if err = sHandler.Store([]byte(id), buf); err != nil {
				m.log.prod.Error("Couldn't persist subscriptions", zap.String("ClientID", id), zap.Error(err))
			}
		}
	} else {
		m.log.prod.Error("Couldn't get subscriber persistence", zap.Error(err))
	}

	m.subscribers = map[string]subscriber.Provider{}

	return nil
}

func (m *Manager) genSessionID() string {
	b := make([]byte, 15)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}

	return base64.URLEncoding.EncodeToString(b)
}

func (m *Manager) onPublish(id string, msg *message.PublishMessage) {
	msg.SetPacketID(0)
	sz, err := msg.Size()
	if err != nil {
		m.log.prod.Error("Couldn't get message size", zap.String("ClientID", id), zap.Error(err))
		return
	}

	buf := make([]byte, sz)
	if _, err = msg.Encode(buf); err != nil {
		m.log.prod.Error("Couldn't encode message", zap.String("ClientID", id), zap.Error(err))
		return
	}

	if err = m.pSessions.PutOutMessage([]byte(id), buf); err != nil {
		m.log.prod.Error("Couldn't persist message", zap.String("ClientID", id), zap.Error(err))
	}
}

func (m *Manager) onStop(id string, protocol message.ProtocolVersion, state *persistenceTypes.SessionState) {
	defer m.sessions.count.Done()

	retain := false
	// non-nil messages object means this is non-clean session
	if state != nil {
		if err := m.pSessions.Store([]byte(id), state); err != nil {
			m.log.prod.Error("Cannot persist session", zap.String("ClientID", id), zap.Error(err))
		}

		retain = true
	} else {
		// It has been unsubscribed by session object
		m.config.Systree.Sessions().Removed()
		m.subsLock.Lock()
		delete(m.subscribers, id)
		m.subsLock.Unlock()
	}

	m.config.Systree.Clients().Disconnected(id, message.CodeSuccess, retain)

	select {
	case <-m.quit:
		// if manager is about to shutdown do nothing
	default:
		m.sessions.lock.Lock()
		delete(m.sessions.list, id)
		m.sessions.lock.Unlock()
	}

	m.log.prod.Info("Client disconnected", zap.String("ClientID", id))
}
