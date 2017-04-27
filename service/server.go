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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/surge/glog"
	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/sessions"
	"github.com/troian/surgemq/topics"
)

// Errors
var (
	ErrInvalidConnectionType error = errors.New("service: Invalid connection type")
	//ErrInvalidSubscriber      error = errors.New("service: Invalid subscriber")
	ErrBufferNotReady         error = errors.New("service: buffer is not ready")
	ErrBufferInsufficientData error = errors.New("service: buffer has insufficient data")
)

// Default configs
const (
	DefaultKeepAlive        = 300           // DefaultKeepAlive default keep
	DefaultConnectTimeout   = 2             // DefaultConnectTimeout connect timeout
	DefaultAckTimeout       = 20            // DefaultAckTimeout ack timeout
	DefaultTimeoutRetries   = 3             // DefaultTimeoutRetries retries
	DefaultSessionsProvider = "mem"         // DefaultSessionsProvider default session provider
	DefaultAuthenticator    = "mockSuccess" // DefaultAuthenticator default auth provider
	DefaultTopicsProvider   = "mem"         // DefaultTopicsProvider default topics provider
)

// Server is a library implementation of the MQTT server that, as best it can, complies
// with the MQTT 3.1 and 3.1.1 specs.
type Server struct {
	// The number of seconds to keep the connection live if there's no data.
	// If not set then default to 5 mins.
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

	// Anonymous either allow anonymous access or not
	Anonymous bool

	// SessionsProvider is the session store that keeps all the Session objects.
	// This is the store to check if CleanSession is set to 0 in the CONNECT message.
	// If not set then default to "mem".
	SessionsProvider string

	// ClientIDFromUser
	ClientIDFromUser bool

	// TopicsProvider is the topic store that keeps all the subscription topics.
	// If not set then default to "mem".
	TopicsProvider string

	// authMgr is the authentication manager that we are going to use for authenticating
	// incoming connections
	authMgr *auth.Manager

	// sessMgr is the sessions manager for keeping track of the sessions
	sessMgr *sessions.Manager

	// topicsMgr is the topics manager for keeping track of subscriptions
	topicsMgr *topics.Manager

	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit chan struct{}

	ln    net.Listener
	lnTLS net.Listener

	// optional TLS config, used by ListenAndServeTLS
	tlsConfig *tls.Config

	// A list of services created by the server. We keep track of them so we can
	// gracefully shut them down if they are still alive when the server goes down.
	svcs []*service

	// Mutex for updating svcs
	mu sync.Mutex

	// A indicator on whether none secure server is running
	running int32

	// A indicator on whether secure server is running
	runningTLS int32

	// A indicator on whether this server has already checked configuration
	configOnce sync.Once

	subs []interface{}
	qoss []byte

	waitServers sync.WaitGroup
}

// clneTLSConfig returns a shallow clone of cfg, or a new zero tls.Config if
// cfg is nil. This is safe to call even if cfg is in active use by a TLS
// client or server.
func cloneTLSConfig(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		return &tls.Config{}
	}
	return cfg.Clone()
}

// NewServer new server
func NewServer() *Server {
	s := Server{
		quit: make(chan struct{}),
	}

	return &s
}

func (s *Server) serve(l net.Listener) error {
	defer l.Close()

	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		conn, err := l.Accept()

		if err != nil {
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
				glog.Errorf("Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		go s.handleConnection(conn)
	}
	//return nil
}

// ListenAndServe listens to connections on the URI requested, and handles any
// incoming MQTT client sessions. It should not return until Close() is called
// or if there's some critical error that stops the server from running. The URI
// supplied should be of the form "protocol://host:port" that can be parsed by
// url.Parse(). For example, an URI could be "tcp://0.0.0.0:1883".
func (s *Server) ListenAndServe(uri string) error {
	defer atomic.CompareAndSwapInt32(&s.running, 1, 0)

	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return errors.New("server/ListenAndServe: Server is already running")
	}

	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	s.waitServers.Add(1)

	defer s.waitServers.Done()

	s.ln, err = net.Listen(u.Scheme, u.Host)
	if err != nil {
		return err
	}

	glog.Infof("server/ListenAndServe: server is ready...")

	return s.serve(s.ln)
}

// ListenAndServeTLS listents to connections on the URI requested, and handles any
// incoming MQTT client sessions. It should not return until Close() is called
// or if there's some critical error that stops the server from running. The URI
// supplied should be of the form "protocol://host:port" that can be parsed by
// url.Parse(). For example, an URI could be "tcp://0.0.0.0:8883".
func (s *Server) ListenAndServeTLS(uri string, certFile, keyFile string) error {
	defer atomic.CompareAndSwapInt32(&s.runningTLS, 1, 0)

	if !atomic.CompareAndSwapInt32(&s.runningTLS, 0, 1) {
		return errors.New("server/ListenAndServeTLS: Server is already running")
	}

	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	config := cloneTLSConfig(s.tlsConfig)
	configHasCert := len(config.Certificates) > 0 || config.GetCertificate != nil
	if !configHasCert || certFile != "" || keyFile != "" {
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return err
		}
	}

	s.waitServers.Add(1)
	defer s.waitServers.Done()

	var ln net.Listener

	ln, err = net.Listen(u.Scheme, u.Host)
	if err != nil {
		return err
	}

	s.lnTLS = tls.NewListener(ln, config)

	glog.Infof("server/ListenAndServeTLS: server is ready...")

	return s.serve(s.lnTLS)
}

// Publish sends a single MQTT PUBLISH message to the server. On completion, the
// supplied OnCompleteFunc is called. For QOS 0 messages, onComplete is called
// immediately after the message is sent to the outgoing buffer. For QOS 1 messages,
// onComplete is called when PUBACK is received. For QOS 2 messages, onComplete is
// called after the PUBCOMP message is received.
func (s *Server) Publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	if err := s.checkConfiguration(); err != nil {
		return err
	}

	if msg.Retain() {
		if err := s.topicsMgr.Retain(msg); err != nil {
			glog.Errorf("Error retaining message: %v", err)
		}
	}

	if err := s.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &s.subs, &s.qoss); err != nil {
		return err
	}

	msg.SetRetain(false)

	//glog.Debugf("(server) Publishing to topic %q and %d subscribers", string(msg.Topic()), len(this.subs))
	for _, s := range s.subs {
		if s != nil {
			if fn, ok := s.(*OnPublishFunc); !ok {
				glog.Errorf("Invalid onPublish Function")
			} else {
				if err := (*fn)(msg); err != nil {
					glog.Errorf("Error onPublish Function")
				}
			}
		}
	}

	return nil
}

// Close terminates the server by shutting down all the client connections and closing
// the listener. It will, as best it can, clean up after itself.
func (s *Server) Close() error {
	// By closing the quit channel, we are telling the server to stop accepting new
	// connection.
	close(s.quit)

	// We then close the net.Listener, which will force Accept() to return if it's
	// blocked waiting for new connections.
	if s.ln != nil {
		s.ln.Close()
	}

	if s.lnTLS != nil {
		s.lnTLS.Close()
	}

	s.waitServers.Wait()

	for _, svc := range s.svcs {
		glog.Infof("Stopping service %d", svc.id)
		svc.stop()
	}

	if s.sessMgr != nil {
		s.sessMgr.Close()
	}

	if s.topicsMgr != nil {
		s.topicsMgr.Close()
	}

	return nil
}

// HandleConnection is for the broker to handle an incoming connection from a client
func (s *Server) handleConnection(c io.Closer) (*service, error) {
	if c == nil {
		return nil, ErrInvalidConnectionType
	}

	var err error

	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	err = s.checkConfiguration()
	if err != nil {
		return nil, err
	}

	conn, ok := c.(net.Conn)
	if !ok {
		return nil, ErrInvalidConnectionType
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

	conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(s.ConnectTimeout)))

	resp := message.NewConnAckMessage()

	svc := &service{
		client:         false,
		connectTimeout: s.ConnectTimeout,
		ackTimeout:     s.AckTimeout,
		timeoutRetries: s.TimeoutRetries,
		conn:           conn,
		sessMgr:        s.sessMgr,
		topicsMgr:      s.topicsMgr,
	}

	var req *message.ConnectMessage

	if req, err = getConnectMessage(conn); err != nil {
		if cerr, ok := err.(message.ConnAckCode); ok {
			//glog.Errorf("request   message: %s\nresponse message: %s\nerror           : %v", mreq, resp, err)
			resp.SetReturnCode(cerr)
		}
	} else {
		if req.UsernameFlag() {
			if s.authMgr.Password(string(req.Username()), string(req.Password())); err == nil {
				resp.SetReturnCode(message.ConnectionAccepted)
			} else {
				resp.SetReturnCode(message.ErrBadUsernameOrPassword)
			}
		} else {
			if s.Anonymous {
				resp.SetReturnCode(message.ConnectionAccepted)
			} else {
				resp.SetReturnCode(message.ErrNotAuthorized)
			}
		}

		if resp.ReturnCode() == message.ConnectionAccepted {
			if req.KeepAlive() == 0 {
				req.SetKeepAlive(minKeepAlive)
			}

			svc.keepAlive = int(req.KeepAlive())
			svc.id = atomic.AddUint64(&gSvcID, 1)

			if err = s.getSession(svc, req, resp); err != nil {
				resp.SetReturnCode(message.ErrServerUnavailable)
				resp.SetSessionPresent(false)
				glog.Errorf("server/handleConnection: session error: %s", err.Error())
			}
		}

		if resp.ReturnCode() != message.ConnectionAccepted {
			resp.SetSessionPresent(false)
		}
	}

	tmpErr := err

	if err = writeMessage(c, resp); err != nil {
		return nil, err
	}

	err = tmpErr

	if resp.ReturnCode() == message.ConnectionAccepted {
		svc.inStat.increment(int64(req.Len()))
		svc.outStat.increment(int64(resp.Len()))

		if err := svc.start(); err != nil {
			svc.stop()
			return nil, err
		}

		//this.mu.Lock()
		//this.svcs = append(this.svcs, svc)
		//this.mu.Unlock()

		glog.Infof("(%s) server/handleConnection: Connection established.", svc.cid())
		return svc, nil
	}

	return nil, err
}

func (s *Server) checkConfiguration() error {
	var err error

	s.configOnce.Do(func() {
		if s.KeepAlive == 0 {
			s.KeepAlive = DefaultKeepAlive
		}

		if s.ConnectTimeout == 0 {
			s.ConnectTimeout = DefaultConnectTimeout
		}

		if s.AckTimeout == 0 {
			s.AckTimeout = DefaultAckTimeout
		}

		if s.TimeoutRetries == 0 {
			s.TimeoutRetries = DefaultTimeoutRetries
		}

		if s.Authenticators == "" {
			s.Authenticators = "mockSuccess"
		}

		s.authMgr, err = auth.NewManager(s.Authenticators)
		if err != nil {
			return
		}

		if s.SessionsProvider == "" {
			s.SessionsProvider = "mem"
		}

		s.sessMgr, err = sessions.NewManager(s.SessionsProvider)
		if err != nil {
			return
		}

		if s.TopicsProvider == "" {
			s.TopicsProvider = "mem"
		}

		s.topicsMgr, err = topics.NewManager(s.TopicsProvider)

		return
	})

	return err
}

func (s *Server) getSession(svc *service, req *message.ConnectMessage, resp *message.ConnAckMessage) error {
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

	if s.ClientIDFromUser {
		req.SetClientId(req.Username())
	}

	// Check to see if the client supplied an ID, if not, generate one and set
	// clean session.
	if len(req.ClientId()) == 0 {
		req.SetClientId([]byte(fmt.Sprintf("internalclient%d", svc.id)))
		req.SetCleanSession(true)
	}

	cid := string(req.ClientId())

	// If CleanSession is NOT set, check the session store for existing session.
	// If found, return it.
	if !req.CleanSession() {
		if svc.sess, err = s.sessMgr.Get(cid); err == nil {
			resp.SetSessionPresent(true)

			if err := svc.sess.Update(req); err != nil {
				return err
			}
		}
	}

	// If CleanSession, or no existing session found, then create a new one
	if svc.sess == nil {
		if svc.sess, err = s.sessMgr.New(cid); err != nil {
			return err
		}

		resp.SetSessionPresent(false)

		if err := svc.sess.Init(req); err != nil {
			return err
		}
	}

	return nil
}
