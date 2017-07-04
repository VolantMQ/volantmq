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
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/troian/surgemq/message"
	persistenceTypes "github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics"
	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
)

var (
	// ErrNotAccepted new connection does not meet requirements
	ErrNotAccepted = errors.New("Connection not accepted")

	// ErrDupNotAllowed case when new client with existing ID connected
	ErrDupNotAllowed = errors.New("duplicate not allowed")
)

// Config manager configuration
type Config struct {
	// Topics manager for all the client subscriptions
	TopicsMgr *topics.Manager

	// The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// The number of seconds to wait for any ACK messages before failing.
	// If not set then default to 20 seconds.
	AckTimeout int

	// The number of times to retry sending a packet if ACK is not received.
	// If no set then default to 3 retries.
	TimeoutRetries int

	Metric struct {
		Packets  systree.PacketsMetric
		Sessions systree.SessionsStat
		Session  systree.SessionStat
	}

	OnDup types.DuplicateConfig

	Persist persistenceTypes.Sessions
}

type sessionsList struct {
	list  map[string]*Type
	lock  sync.RWMutex
	count sync.WaitGroup
}

// Manager interface
type Manager struct {
	config   Config
	sessions struct {
		active    sessionsList
		suspended sessionsList
	}
	lock sync.Mutex
	quit chan struct{}
}

// NewManager alloc new
func NewManager(cfg Config) (*Manager, error) {
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

	m.sessions.active.list = make(map[string]*Type)
	m.sessions.suspended.list = make(map[string]*Type)

	// 1. load persisted sessions
	persistedSessions, err := m.config.Persist.GetAll()
	if err == nil {
		for _, s := range persistedSessions {
			// 2. restore only those having persisted subscriptions
			if persistedSubs, err := s.Subscriptions(); err == nil {
				var subscriptions message.TopicsQoS
				if subscriptions, err = persistedSubs.Get(); err == nil && len(subscriptions) > 0 {
					var sID string
					if sID, err = s.ID(); err != nil {
						logger.Error("Couldn't get persisted session ID", zap.Error(err))
					} else {
						sCfg := config{
							topicsMgr:      m.config.TopicsMgr,
							connectTimeout: m.config.ConnectTimeout,
							ackTimeout:     m.config.AckTimeout,
							timeoutRetries: m.config.TimeoutRetries,
							subscriptions:  subscriptions,
							id:             sID,
							callbacks: managerCallbacks{
								onDisconnect: m.onDisconnect,
								onStop:       m.onStop,
								onPublish:    m.onPublish,
							},
						}

						sCfg.metric.session = m.config.Metric.Session
						sCfg.metric.packets = m.config.Metric.Packets

						var ses *Type
						if ses, err = newSession(sCfg); err != nil {
							logger.Error("Couldn't start persisted session", zap.String("ClientID", sID), zap.Error(err))
						} else {
							m.sessions.suspended.list[sID] = ses
							m.sessions.suspended.count.Add(1)
							if err = persistedSubs.Delete(); err != nil {
								logger.Error("Couldn't wipe subscriptions after restore", zap.String("ClientID", sID), zap.Error(err))
							}
						}
					}
				}
			} else {
				//logger.Error(zap.Error(err))
			}
		}
	}

	return m, nil
}

// Start try start new session
func (m *Manager) Start(msg *message.ConnectMessage, resp *message.ConnAckMessage, conn io.Closer) error {
	var err error
	var ses *Type
	present := false

	defer func() {
		resp.SetSessionPresent(present)

		if err = m.writeMessage(conn, resp); err != nil {
			logger.Error("Couldn't write CONNACK", zap.Error(err))
		}
		if err == nil {
			if ses != nil {
				// try start session
				if err = ses.start(msg, conn); err != nil {
					// should never get into this section.
					// if so this code does not work as expected :)
					logger.Error("Something really bad happened", zap.Error(err))
				}
			}
		}
	}()

	select {
	case <-m.quit:
		resp.SetReturnCode(message.ErrServerUnavailable)
		return errors.New("Not running")
	default:
	}

	if resp.ReturnCode() != message.ConnectionAccepted {
		return ErrNotAccepted
	}

	// serialize access to multiple starts
	defer m.lock.Unlock()
	m.lock.Lock()

	id := string(msg.ClientID())
	if len(id) == 0 {
		id = m.genSessionID()
	}

	m.sessions.active.lock.RLock()

	alloc := true
	// there is no such active session
	// proceed to either persisted or new one
	if s, ok := m.sessions.active.list[id]; ok {
		ses = s
	}
	m.sessions.active.lock.RUnlock()

	if ses != nil {
		replaced := true
		// session already exists thus duplicate case happened
		if !m.config.OnDup.Replace {
			// duplicate prohibited. send identifier rejected
			resp.SetReturnCode(message.ErrIdentifierRejected)
			err = ErrDupNotAllowed
			replaced = false
			alloc = false
		} else {
			// duplicate allowed stop current session
			ses.stop(true)
		}

		// notify subscriber about dup attempt
		if m.config.OnDup.OnAttempt != nil {
			m.config.OnDup.OnAttempt(id, replaced)
		}
	}

	if alloc {
		ses, present, err = m.allocSession(id, msg, resp)
	}

	return nil
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
	m.sessions.active.lock.Lock()
	for _, s := range m.sessions.active.list {
		s.disconnect()
	}
	m.sessions.active.lock.Unlock()

	// 2. Wait until all active sessions stopped
	m.sessions.active.count.Wait()

	// 3. wipe list
	m.sessions.active.list = make(map[string]*Type)

	// 4. Signal suspended sessions to exit
	for _, s := range m.sessions.suspended.list {
		s.stop(false)
	}

	// 2. Wait until suspended sessions stopped
	m.sessions.suspended.count.Wait()

	// 4. wipe list
	m.sessions.suspended.list = make(map[string]*Type)

	return nil
}

func (m *Manager) genSessionID() string {
	b := make([]byte, 15)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}

	return base64.URLEncoding.EncodeToString(b)
}

func (m *Manager) allocSession(id string, msg *message.ConnectMessage, resp *message.ConnAckMessage) (*Type, bool, error) {
	var ses *Type
	present := false
	var err error

	sConfig := config{
		topicsMgr:      m.config.TopicsMgr,
		connectTimeout: m.config.ConnectTimeout,
		ackTimeout:     m.config.AckTimeout,
		timeoutRetries: m.config.TimeoutRetries,
		subscriptions:  make(message.TopicsQoS),
		id:             id,
		callbacks: managerCallbacks{
			onDisconnect: m.onDisconnect,
			onStop:       m.onStop,
			onPublish:    m.onPublish,
		},
	}

	sConfig.metric.session = m.config.Metric.Session
	sConfig.metric.packets = m.config.Metric.Packets

	var pSes persistenceTypes.Session

	// session may be persisted before
	m.sessions.suspended.lock.Lock()
	if s, ok := m.sessions.suspended.list[id]; ok {
		// session exists. acquire it
		delete(m.sessions.suspended.list, id)

		if msg.CleanSession() {
			// client may want clear previously persisted state. If client with same ID is clean
			// delete all persisted data

			s.stop(false)
			if err = m.config.Persist.Delete(id); err != nil {
				dLogger.Debug("Couldn't wipe session after restore", zap.String("ClientID", id), zap.Error(err))
			}
		} else {
			if s != nil && s.isOpen() {
				ses = s
				present = true
				// do not check error here.
				// if session has not been found there is no any persisted messages for it
				pSes, _ = m.config.Persist.Get(id)
				m.sessions.suspended.count.Done()
			}
		}
	}
	m.sessions.suspended.lock.Unlock()

	if ses == nil {
		// no such session in persisted list. It might be shutdown
		if pSes, err = m.config.Persist.Get(id); err != nil {
			// No such session exists at all. Just create new
			dLogger.Debug("Create new persist entry", zap.String("ClientID", id))
			if _, err = m.config.Persist.New(id); err != nil {
				logger.Error("Couldn't create persis object for session", zap.String("ClientID", id), zap.Error(err))
			}
		} else {
			// Session exists and is in shutdown state
			dLogger.Debug("Restore session from shutdown", zap.String("ClientID", id))
			present = true
		}

		if ses, err = newSession(sConfig); err != nil {
			ses = nil
			resp.SetReturnCode(message.ErrServerUnavailable)
			if !msg.CleanSession() {
				if err = m.config.Persist.Delete(id); err != nil {
					logger.Error("Couldn't wipe session after restore", zap.String("ClientID", id), zap.Error(err))
				}
			}
		}
	}

	if ses != nil {
		// restore messages if it was shutdown non-clean session
		if pSes != nil {
			var sesMessages persistenceTypes.Messages
			if sesMessages, err = pSes.Messages(); err == nil {
				var storedMessages *persistenceTypes.SessionMessages
				if storedMessages, err = sesMessages.Load(); err == nil {
					ses.restore(storedMessages)
					if err = sesMessages.Delete(); err != nil {
						logger.Error("Couldn't wipe messages after restore", zap.String("ClientID", id), zap.Error(err))
					}
				}
			}
		}

		m.sessions.active.lock.Lock()
		m.sessions.active.list[id] = ses
		m.sessions.active.lock.Unlock()
		m.sessions.active.count.Add(1)
	}

	return ses, present, err
}

// onStop is only invoked for non-clean session
func (m *Manager) onStop(id string, s message.TopicsQoS) {
	defer m.sessions.suspended.count.Done()

	ses, err := m.config.Persist.Get(id)
	if err != nil {
		logger.Error("Trying to persist session that has not been initiated for persistence", zap.String("ClientID", id), zap.Error(err))
	} else {
		var sesSubs persistenceTypes.Subscriptions
		if sesSubs, err = ses.Subscriptions(); err == nil {
			if err = sesSubs.Add(s); err != nil {
				logger.Error("Couldn't persist subscriptions", zap.String("ClientID", id), zap.Error(err))
			}
		} else {
			logger.Error("Error", zap.Error(err))
		}
	}
}

func (m *Manager) onPublish(id string, msg *message.PublishMessage) {
	if ses, err := m.config.Persist.Get(id); err == nil {
		var sesMsg persistenceTypes.Messages
		if sesMsg, err = ses.Messages(); err == nil {
			if err = sesMsg.Store("out", []message.Provider{msg}); err != nil {
				logger.Error("Couldn't store messages", zap.String("ClientID", id), zap.Error(err))
			}
		} else {
			logger.Error("Couldn't store messages", zap.String("ClientID", id), zap.Error(err))
		}
	} else {
		logger.Error("Couldn't persist message for shutdown session", zap.String("ClientID", id), zap.Error(err))
	}
}

func (m *Manager) onDisconnect(id string, messages *persistenceTypes.SessionMessages, shutdown bool) {
	defer m.sessions.active.count.Done()

	// non-nil messages object means this is non-clean session
	if messages != nil {
		// persist messages if any
		if ses, err := m.config.Persist.Get(id); err != nil {
			logger.Error("Trying to persist session that has not been initiated for persistence", zap.String("ClientID", id), zap.Error(err))
		} else {
			var sesMsg persistenceTypes.Messages
			if sesMsg, err = ses.Messages(); err == nil {
				if len(messages.Out.Messages) > 0 {
					if err = sesMsg.Store("out", messages.Out.Messages); err != nil {
						logger.Error("Couldn't persist messages", zap.String("ClientID", id), zap.Error(err))
					}
				}

				if len(messages.In.Messages) > 0 {
					if err = sesMsg.Store("in", messages.In.Messages); err != nil {
						logger.Error("Couldn't persist messages", zap.String("ClientID", id), zap.Error(err))
					}
				}
			} else {
				logger.Error("Couldn't persist messages", zap.String("ClientID", id), zap.Error(err))
			}
		}

		// if session has active subscriptions move it to suspended place
		if !shutdown {
			m.sessions.suspended.lock.Lock()
			m.sessions.active.lock.RLock()
			m.sessions.suspended.list[id] = m.sessions.active.list[id]
			m.sessions.active.lock.RUnlock()
			m.sessions.suspended.count.Add(1)
			m.sessions.suspended.lock.Unlock()
		}
	}

	select {
	case <-m.quit:
		// if manager is about to shutdown do nothing
	default:
		m.sessions.active.lock.Lock()
		delete(m.sessions.active.list, id)
		m.sessions.active.lock.Unlock()
	}
}

// WriteMessage into connection
func (m *Manager) writeMessage(conn io.Closer, msg message.Provider) error {
	buf := make([]byte, msg.Len())
	_, err := msg.Encode(buf)
	if err != nil {
		dLogger.Debug("Write error", zap.Error(err))
		return err
	}
	//appLog.Debugf("Writing: %s", msg)

	return m.writeMessageBuffer(conn, buf)
}

func (m *Manager) writeMessageBuffer(c io.Closer, b []byte) error {
	if c == nil {
		return types.ErrInvalidConnectionType
	}

	conn, ok := c.(net.Conn)
	if !ok {
		return types.ErrInvalidConnectionType
	}

	_, err := conn.Write(b)
	return err
}
