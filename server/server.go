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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/loggo"
	"github.com/troian/surgemq"
	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/service"
	"github.com/troian/surgemq/session"
	"github.com/troian/surgemq/topics"
	"strconv"
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
}

type servicesManager struct {
	svcID uint64
	// Mutex for updating services
	lock sync.Mutex
	// A list of services created by the server. We keep track of them so we can
	// gracefully shut them down if they are still alive when the server goes down.
	list map[uint64]*service.Type
}

func (sm *servicesManager) IncID() uint64 {
	return atomic.AddUint64(&sm.svcID, 1)
}

func (sm *servicesManager) insert(svc *service.Type) {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	sm.list[svc.ID] = svc
}

// Listener listener
type Listener struct {
	Scheme      string
	Host        string
	Port        int
	CertFile    string
	KeyFile     string
	AuthManager *auth.Manager
	listener    net.Listener
	tlsConfig   *tls.Config
	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit chan struct{}
}

// Type is a library implementation of the MQTT server that, as best it can, complies
// with the MQTT 3.1 and 3.1.1 specs.
type Type struct {
	config Config

	// authMgr is the authentication manager that we are going to use for authenticating
	// incoming connections
	authMgr *auth.Manager

	// sessionsMgr is the sessions manager for keeping track of the sessions
	sessionsMgr *session.Manager

	// topicsMgr is the topics manager for keeping track of subscriptions
	topicsMgr *topics.Manager

	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit chan struct{}

	listeners   map[int]*Listener
	wgListeners sync.WaitGroup
	lLock       sync.Mutex

	subs []interface{}
	qoss []message.QosType

	wgStopped     sync.WaitGroup
	wgConnections sync.WaitGroup

	//services sync.Map
	services servicesManager
}

var appLog loggo.Logger

func init() {
	appLog = loggo.GetLogger("mq.server")
	appLog.SetLogLevel(loggo.TRACE)
}

// New new server
func New(config Config) (*Type, error) {
	s := Type{
		config:    config,
		quit:      make(chan struct{}),
		listeners: make(map[int]*Listener),
		services: servicesManager{
			list: make(map[uint64]*service.Type),
		},
	}

	if s.config.KeepAlive == 0 {
		s.config.KeepAlive = surgemq.DefaultAckTimeout
	}

	if s.config.ConnectTimeout == 0 {
		s.config.ConnectTimeout = surgemq.DefaultConnectTimeout
	}

	if s.config.AckTimeout == 0 {
		s.config.AckTimeout = surgemq.DefaultAckTimeout
	}

	if s.config.TimeoutRetries == 0 {
		s.config.TimeoutRetries = surgemq.DefaultTimeoutRetries
	}

	if s.config.Authenticators == "" {
		s.config.Authenticators = "mockSuccess"
	}

	var err error
	s.authMgr, err = auth.NewManager(s.config.Authenticators)
	if err != nil {
		return nil, err
	}

	s.sessionsMgr, err = session.NewManager()
	if err != nil {
		return nil, err
	}

	if s.config.TopicsProvider == "" {
		s.config.TopicsProvider = "mem"
	}

	s.topicsMgr, err = topics.NewManager(s.config.TopicsProvider)
	if err != nil {
		return nil, err
	}

	//s.servicesExit = make(chan uint64, 1024)

	//s.wgStopped.Add(1)
	//go s.processDisconnects()

	return &s, nil
}

// ListenAndServe listens to connections on the URI requested, and handles any
// incoming MQTT client sessions. It should not return until Close() is called
// or if there's some critical error that stops the server from running. The URI
// supplied should be of the form "protocol://host:port" that can be parsed by
// url.Parse(). For example, an URI could be "tcp://0.0.0.0:1883".
func (s *Type) ListenAndServe(listener *Listener) error {
	var err error

	if listener.CertFile != "" && listener.KeyFile != "" {
		listener.tlsConfig = &tls.Config{
			Certificates: make([]tls.Certificate, 1),
		}

		listener.tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(listener.CertFile, listener.KeyFile)
		if err != nil {
			listener.tlsConfig = nil
			return err
		}

	}

	var ln net.Listener
	if ln, err = net.Listen(listener.Scheme, listener.Host+":"+strconv.Itoa(listener.Port)); err != nil {
		return err
	}

	if listener.tlsConfig != nil {
		listener.listener = tls.NewListener(ln, listener.tlsConfig)
	} else {
		listener.listener = ln
	}

	s.lLock.Lock()
	if _, ok := s.listeners[listener.Port]; !ok {
		listener.quit = s.quit
		s.listeners[listener.Port] = listener
		s.lLock.Unlock()

		s.wgListeners.Add(1)
		defer s.wgListeners.Done()

		appLog.Infof("mqtt server on [%s://%s:%d] is ready...", listener.Scheme, listener.Host, listener.Port)

		err = s.serve(listener)

		appLog.Infof("mqtt server on [%s://%s:%d] stopped", listener.Scheme, listener.Host, listener.Port)
	} else {
		s.lLock.Unlock()
		err = errors.New("Listener already exists")
	}

	return err
}

// Publish sends a single MQTT PUBLISH message to the server. On completion, the
// supplied OnCompleteFunc is called. For QOS 0 messages, onComplete is called
// immediately after the message is sent to the outgoing buffer. For QOS 1 messages,
// onComplete is called when PUBACK is received. For QOS 2 messages, onComplete is
// called after the PUBCOMP message is received.
func (s *Type) Publish(msg *message.PublishMessage, onComplete surgemq.OnCompleteFunc) error {
	if msg.Retain() {
		if err := s.topicsMgr.Retain(msg); err != nil {
			appLog.Errorf("Error retaining message: %v", err)
		}
	}

	if err := s.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &s.subs, &s.qoss); err != nil {
		return err
	}

	msg.SetRetain(false)

	for _, s := range s.subs {
		if s != nil {
			if fn, ok := s.(*surgemq.OnPublishFunc); !ok {
				appLog.Errorf("Invalid onPublish Function")
			} else {
				if err := (*fn)(msg); err != nil {
					appLog.Errorf("Error onPublish Function")
				}
			}
		}
	}

	return nil
}

// Close terminates the server by shutting down all the client connections and closing
// the listener. It will, as best it can, clean up after itself.
func (s *Type) Close() error {
	// By closing the quit channel, we are telling the server to stop accepting new
	// connection.
	close(s.quit)

	// We then close all net.Listener, which will force Accept() to return if it's
	// blocked waiting for new connections.

	for _, l := range s.listeners {
		if err := l.listener.Close(); err != nil {
			appLog.Errorf(err.Error())
		}
	}

	s.wgListeners.Wait()
	s.wgConnections.Wait()

	s.services.lock.Lock()
	for id, svc := range s.services.list {
		svc.Stop()
		delete(s.services.list, id)
	}

	s.services.lock.Unlock()

	s.wgStopped.Wait()

	if s.sessionsMgr != nil {
		s.sessionsMgr.Shutdown() // nolint: errcheck
	}

	if s.topicsMgr != nil {
		s.topicsMgr.Close() // nolint: errcheck
	}

	return nil
}

func (s *Type) serve(l *Listener) error {
	defer func() {
		l.listener.Close() // nolint: errcheck
	}()

	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		var conn net.Conn
		var err error

		if conn, err = l.listener.Accept(); err != nil {
			// http://zhen.org/blog/graceful-shutdown-of-go-net-dot-listeners/
			select {
			case <-s.quit:
				return nil
			default:
			}

			// Borrowed from go1.3.3/src/pkg/net/http/server.go:1699
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				appLog.Errorf("Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		s.wgConnections.Add(1)
		go func() {
			defer s.wgConnections.Done()
			s.handleConnection(conn, l.AuthManager) // nolint: errcheck
		}()
	}
}

// handleConnection is for the broker to handle an incoming connection from a client
func (s *Type) handleConnection(c io.Closer, authMng *auth.Manager) error {
	if c == nil {
		return surgemq.ErrInvalidConnectionType
	}

	var err error

	defer func() {
		if err != nil {
			c.Close() // nolint: errcheck
		}
	}()

	conn, ok := c.(net.Conn)
	if !ok {
		return surgemq.ErrInvalidConnectionType
	}

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

	//conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(s.config.ConnectTimeout))) // nolint: errcheck

	resp := message.NewConnAckMessage()

	svc, _ := service.NewService(service.Config{
		ConnectTimeout: s.config.ConnectTimeout,
		AckTimeout:     s.config.AckTimeout,
		TimeoutRetries: s.config.TimeoutRetries,
		Conn:           conn,
		SessionMgr:     s.sessionsMgr,
		TopicsMgr:      s.topicsMgr,
		OnClose:        s.onSessionClose,
	})

	var req *message.ConnectMessage

	if req, err = service.GetConnectMessage(conn); err != nil {
		if code, ok := message.ValidConnAckError(err); ok {
			resp.SetReturnCode(code)
		} else {
			appLog.Warningf("Couldn't read connect message: %s", err.Error())
			return err
		}
	} else {
		if req.UsernameFlag() {
			if err = authMng.Password(string(req.Username()), string(req.Password())); err == nil {
				resp.SetReturnCode(message.ConnectionAccepted)
			} else {
				resp.SetReturnCode(message.ErrBadUsernameOrPassword)
			}
		} else {
			if s.config.Anonymous {
				resp.SetReturnCode(message.ConnectionAccepted)
			} else {
				resp.SetReturnCode(message.ErrNotAuthorized)
			}
		}

		if resp.ReturnCode() == message.ConnectionAccepted {
			if req.KeepAlive() == 0 {
				req.SetKeepAlive(surgemq.MinKeepAlive)
			}

			svc.KeepAlive = int(req.KeepAlive())
			svc.ID = s.services.IncID()

			if err = s.getSession(svc, req, resp); err != nil {
				resp.SetReturnCode(message.ErrServerUnavailable)
				resp.SetSessionPresent(false)
				appLog.Errorf("server/handleConnection: session error: %s", err.Error())
			}
		}

		if resp.ReturnCode() != message.ConnectionAccepted {
			resp.SetSessionPresent(false)
		}
	}

	tmpErr := err

	if err = service.WriteMessage(c, resp); err != nil {
		return err
	}

	err = tmpErr

	if resp.ReturnCode() == message.ConnectionAccepted {
		if err = svc.Start(int64(req.Len()), int64(resp.Len())); err != nil {
			svc.Stop()
			return err
		}

		s.services.insert(svc)

		appLog.Debugf("[%s] new connection established", svc.CID())
		return nil
	}

	return err
}

func (s *Type) getSession(svc *service.Type, req *message.ConnectMessage, resp *message.ConnAckMessage) error {
	// If CleanSession is set to 0, the server MUST resume communications with the
	// client based on state from the current session, as identified by the client
	// identifier. If there is no session associated with the client identifier the
	// server must create a new session.
	//
	// If CleanSession is set to 1, the client and server must discard any previous
	// session and start a new one. This session lasts as long as the network
	// connection. State data associated with this session must not be reused in any
	// subsequent session.

	var err error

	// Check to see if the client supplied an ID, if not, generate one and set
	// clean session.
	if len(req.ClientID()) == 0 {
		req.SetClientID([]byte(fmt.Sprintf("internalclient%d", svc.ID))) // nolint: errcheck
		req.SetCleanSession(true)
	}

	if s.config.ClientIDFromUser {
		req.SetClientID(req.Username()) // nolint: errcheck
	}

	clientID := string(req.ClientID())

	var ses *session.Type
	// If CleanSession is NOT set, check the session store for existing session.
	// If found, return it.
	if !req.CleanSession() {
		if ses, err = s.sessionsMgr.Get(clientID); err == nil {
			resp.SetSessionPresent(true)

			if err := ses.Update(req); err != nil {
				return err
			}

			svc.SetSession(ses)
		}
	}

	// If CleanSession, or no existing session found, then create a new one
	if ses == nil {
		if ses, err = s.sessionsMgr.New(clientID); err != nil {
			return err
		}

		resp.SetSessionPresent(false)

		if err := ses.Init(req); err != nil {
			return err
		}

		svc.SetSession(ses)
	}

	return nil
}

func (s *Type) onSessionClose(id uint64) {
	select {
	case <-s.quit:
		return
	default:
	}

	s.services.lock.Lock()
	defer s.services.lock.Unlock()

	if _, ok := s.services.list[id]; ok {
		delete(s.services.list, id)
	}
}
