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

package server

import (
	"errors"
	"sync"

	"go.uber.org/zap"

	"strconv"

	"time"

	"github.com/troian/surgemq"
	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence"
	persistTypes "github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/session"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics"
	types "github.com/troian/surgemq/types"
)

// Config server configuration
type Config struct {
	// The number of seconds to keep the connection live if there's no data.
	// If not set then default to 5 minutes.
	KeepAlive int

	// The number of seconds to wait for the CONNECT message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// The number of seconds to wait for any ACK messages before failing.
	// If not set then default to 20 seconds.
	AckTimeout int

	// The number of times to retry sending a packet if ACK is not received.
	// If no set then default to 3 retries.
	TimeoutRetries int

	// Authenticator is the authenticator used to check username and password sent
	// in the CONNECT message. If not set then default to "mockSuccess".
	Authenticators string

	// TopicsProvider is the topic store that keeps all the subscription topics.
	// If not set then default to "mem".
	TopicsProvider string

	// Anonymous either allow anonymous access or not
	Anonymous bool

	// ClientIDFromUser
	ClientIDFromUser bool

	// Persistence config of persistence provider
	Persistence persistTypes.ProviderConfig

	// DupConfig behaviour of server when client with existing ID tries connect
	DupConfig types.DuplicateConfig

	ListenerStatus func(id string, start bool)
}

type listenerInner struct {
	config Config
	// authMgr is the authentication manager that we are going to use for authenticating
	// incoming connections
	authMgr *auth.Manager

	// sessionsMgr is the sessions manager for keeping track of the sessions
	sessionsMgr *session.Manager

	// topicsMgr is the topics manager for keeping track of subscriptions
	topicsMgr *topics.Manager

	persist persistTypes.Provider

	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit chan struct{}

	lock sync.Mutex

	listeners struct {
		list map[int]Listener
		wg   sync.WaitGroup
	}

	wgConnections sync.WaitGroup

	sysTree systree.Provider
}

// ListenerBase base configuration object for listeners
type ListenerBase struct {
	Port        int
	CertFile    string
	KeyFile     string
	AuthManager *auth.Manager

	inner *listenerInner
	log   types.LogInterface
}

// Listener listener
type Listener interface {
	listenerProtocol() string
	start() error
	close() error
}

// Type server API
type Type interface {
	ListenAndServe(listener Listener) error
	Close() error
}

// Type is a library implementation of the MQTT server that, as best it can, complies
// with the MQTT 3.1 and 3.1.1 specs.
type implementation struct {
	log types.LogInterface

	inner listenerInner
}

// New new server
func New(config Config) (Type, error) {
	s := &implementation{}

	s.inner.config = config

	s.log.Prod = surgemq.GetProdLogger().Named("server")
	s.log.Dev = surgemq.GetDevLogger().Named("server")

	s.inner.quit = make(chan struct{})
	s.inner.listeners.list = make(map[int]Listener)

	if s.inner.config.KeepAlive == 0 {
		s.inner.config.KeepAlive = types.DefaultAckTimeout
	}

	if s.inner.config.ConnectTimeout == 0 {
		s.inner.config.ConnectTimeout = types.DefaultConnectTimeout
	}

	if s.inner.config.AckTimeout == 0 {
		s.inner.config.AckTimeout = types.DefaultAckTimeout
	}

	if s.inner.config.TimeoutRetries == 0 {
		s.inner.config.TimeoutRetries = types.DefaultTimeoutRetries
	}

	if s.inner.config.Authenticators == "" {
		s.inner.config.Authenticators = "mockSuccess"
	}

	var err error
	if s.inner.authMgr, err = auth.NewManager(s.inner.config.Authenticators); err != nil {
		return nil, err
	}

	if s.inner.sysTree, err = systree.NewTree(); err != nil {
		return nil, err
	}

	if s.inner.config.Persistence == nil {
		return nil, errors.New("Persistence provider cannot be nil")
	}

	if s.inner.persist, err = persistence.New(s.inner.config.Persistence); err != nil {
		return nil, err
	}

	var persisRetained persistTypes.Retained

	persisRetained, _ = s.inner.persist.Retained()

	tConfig := topics.Config{
		Name:    s.inner.config.TopicsProvider,
		Stat:    s.inner.sysTree.Topics(),
		Persist: persisRetained,
	}
	if s.inner.topicsMgr, err = topics.NewManager(tConfig); err != nil {
		return nil, err
	}

	var persisSession persistTypes.Sessions

	persisSession, _ = s.inner.persist.Sessions()

	mConfig := session.Config{
		TopicsMgr:      s.inner.topicsMgr,
		ConnectTimeout: s.inner.config.ConnectTimeout,
		AckTimeout:     s.inner.config.AckTimeout,
		TimeoutRetries: s.inner.config.TimeoutRetries,
		Persist:        persisSession,
		OnDup:          s.inner.config.DupConfig,
	}
	mConfig.Metric.Packets = s.inner.sysTree.Metric().Packets()
	mConfig.Metric.Session = s.inner.sysTree.Session()
	mConfig.Metric.Sessions = s.inner.sysTree.Sessions()

	if s.inner.sessionsMgr, err = session.NewManager(mConfig); err != nil {
		return nil, err
	}

	if s.inner.config.TopicsProvider == "" {
		s.inner.config.TopicsProvider = "mem"
	}

	return s, nil
}

func (s *implementation) ListenAndServe(listener Listener) error {
	var err error

	switch l := listener.(type) {
	case *ListenerTCP:
		l.inner = &s.inner
		l.log.Prod = s.log.Prod.Named("tcp").Named(strconv.Itoa(l.Port))
		l.log.Dev = s.log.Dev.Named("tcp").Named(strconv.Itoa(l.Port))
		err = l.start()
	case *ListenerWS:
		l.inner = &s.inner
		l.log.Prod = s.log.Prod.Named("ws").Named(strconv.Itoa(l.Port))
		l.log.Dev = s.log.Dev.Named("ws").Named(strconv.Itoa(l.Port))
		err = l.start()
	default:
		err = errors.New("Invalid listener type")
	}

	return err
}

// Close terminates the server by shutting down all the client connections and closing
// the listener. It will, as best it can, clean up after itself.
func (s *implementation) Close() error {
	// By closing the quit channel, we are telling the server to stop accepting new
	// connection.
	select {
	case <-s.inner.quit:
		return nil
	default:
		close(s.inner.quit)
	}

	defer s.inner.lock.Unlock()
	s.inner.lock.Lock()

	// We then close all net.Listener, which will force Accept() to return if it's
	// blocked waiting for new connections.
	for _, l := range s.inner.listeners.list {
		if err := l.close(); err != nil {
			s.log.Prod.Error(err.Error())
		}
	}

	// if there are any new connection in progress lets wait until they are finished
	s.inner.wgConnections.Wait()

	// Wait all of listeners has finished
	s.inner.listeners.wg.Wait()

	for port := range s.inner.listeners.list {
		delete(s.inner.listeners.list, port)
	}

	if s.inner.sessionsMgr != nil {
		if s.inner.persist != nil {
			s.inner.sessionsMgr.Shutdown() // nolint: errcheck, gas
		}
	}

	if s.inner.topicsMgr != nil {
		s.inner.topicsMgr.Close() // nolint: errcheck, gas
	}

	return nil
}

// handleConnection is for the broker to handle an incoming connection from a client
func (l *ListenerBase) handleConnection(c types.Conn) error {
	if c == nil {
		return types.ErrInvalidConnectionType
	}

	var err error

	defer func() {
		if err != nil {
			c.Close() // nolint: errcheck, gas
			c = nil
		}
	}()

	// To establish a connection, we must
	// 1. Read and decode the message.ConnectMessage from the wire
	// 2. If no decoding errors, then authenticate using username and password.
	//    Otherwise, write out to the wire message.ConnackMessage with
	//    appropriate error.
	// 3. If authentication is successful, then either create a new session or
	//    retrieve existing session
	// 4. Write out to the wire a successful message.ConnackMessage message

	// Read the CONNECT message from the wire, if error, then check to see if it's
	// a CONNACK error. If it's CONNACK error, send the proper CONNACK error back
	// to client. Exit regardless of error type.

	c.SetReadDeadline(time.Now().Add(time.Second * time.Duration(l.inner.config.ConnectTimeout))) // nolint: errcheck, gas

	resp := message.NewConnAckMessage()

	var req message.Provider

	var buf []byte
	if buf, err = GetMessageBuffer(c); err != nil {
		return err
	}

	if req, _, err = message.Decode(buf); err != nil {
		if code, ok := message.ValidConnAckError(err); ok {
			l.inner.sysTree.Metric().Packets().Received(resp.Type())
			resp.SetReturnCode(code)

			if err = WriteMessage(c, resp); err != nil {
				return err
			}
			l.inner.sysTree.Metric().Packets().Sent(resp.Type())
		} else {
			//l.log.Prod.Warn("Couldn't read connect message", zap.Error(err))
			return err
		}
	} else {
		switch r := req.(type) {
		case *message.ConnectMessage:
			if r.UsernameFlag() {
				if err = l.AuthManager.Password(string(r.Username()), string(r.Password())); err == nil {
					resp.SetReturnCode(message.ConnectionAccepted)
				} else {
					resp.SetReturnCode(message.ErrBadUsernameOrPassword)
				}
			} else {
				if l.inner.config.Anonymous {
					resp.SetReturnCode(message.ConnectionAccepted)
				} else {
					resp.SetReturnCode(message.ErrNotAuthorized)
				}
			}

			if r.KeepAlive() == 0 {
				r.SetKeepAlive(uint16(l.inner.config.KeepAlive))
			}
			if err = l.inner.sessionsMgr.Start(r, resp, c); err != nil {
				if err != session.ErrNotAccepted {
					l.log.Prod.Error("Couldn't start session", zap.Error(err))
				}
			}
		default:
			return errors.New("Invalid message type")
		}
	}

	return err
}
