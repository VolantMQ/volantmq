package surgemq

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/configuration"
	"github.com/troian/surgemq/listener"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence"
	persistTypes "github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/routines"
	"github.com/troian/surgemq/session"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics"
	"github.com/troian/surgemq/topics/types"
	"github.com/troian/surgemq/types"

	"go.uber.org/zap"
)

// ServerConfig configuration of the MQTT server
type ServerConfig struct {
	// The number of seconds to keep the connection live if there's no data.
	// If not set then default to 5 minutes.
	KeepAlive int

	// The number of seconds to wait for the CONNECT message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// Authenticator is the authenticator used to check username and password sent
	// in the CONNECT message. If not set then default to "mockSuccess".
	Authenticators string

	// Configuration of persistence provider
	Persistence persistTypes.ProviderConfig

	// OnDuplicate If requested we notify if there is attempt to dup session
	OnDuplicate func(string, bool)

	// ListenerStatus user provided callback to track listener status
	ListenerStatus func(id string, status string)

	// AllowedVersions
	AllowedVersions []message.ProtocolVersion

	// Anonymous either allow or deny anonymous access
	// Default is false
	Anonymous bool

	// OfflineQoS0 tell server to either persist (true) or not persist (false) QoS 0 messages for non-clean sessions
	// Default is false
	OfflineQoS0 bool

	// AllowDuplicates Either allow or deny replacing of existing session if there new client with same clientID
	// default false
	AllowDuplicates bool
}

// NewServerConfig with default values
func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		KeepAlive:       types.DefaultAckTimeout,
		ConnectTimeout:  types.DefaultConnectTimeout,
		Authenticators:  "mockSuccess",
		Persistence:     persistTypes.MemConfig{},
		Anonymous:       false,
		OfflineQoS0:     false,
		AllowDuplicates: false,
		AllowedVersions: []message.ProtocolVersion{
			message.ProtocolV31,
			message.ProtocolV311,
		},
	}
}

// Server server API
type Server interface {
	ListenAndServe(listener.Config) error
	Close() error
}

// Type is a library implementation of the MQTT server that, as best it can, complies
// with the MQTT 3.1/3.1.1 and 5.0 specs.
type implementation struct {
	log types.LogInterface

	config *ServerConfig
	// authMgr is the authentication manager that we are going to use for authenticating
	// incoming connections
	authMgr *auth.Manager

	// sessionsMgr is the sessions manager for keeping track of the sessions
	sessionsMgr *sessions.Manager

	// topicsMgr is the topics manager for keeping track of subscriptions
	topicsMgr topicsTypes.Provider

	persist persistTypes.Provider

	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit chan struct{}

	lock sync.Mutex

	listeners struct {
		list map[int]listener.Provider
		wg   sync.WaitGroup
	}

	sysTree systree.Provider

	onClose sync.Once

	// nodes cluster nodes
	//nodes map[string]subscriber.Provider
}

// NewServer allocate server object
func NewServer(config *ServerConfig) (Server, error) {
	s := &implementation{}

	s.config = config

	s.log.Prod = configuration.GetProdLogger().Named("server")
	s.log.Dev = configuration.GetDevLogger().Named("server")

	s.quit = make(chan struct{})
	s.listeners.list = make(map[int]listener.Provider)

	var err error
	if s.authMgr, err = auth.NewManager(s.config.Authenticators); err != nil {
		return nil, err
	}

	if s.sysTree, err = systree.NewTree(); err != nil {
		return nil, err
	}

	if s.config.Persistence == nil {
		return nil, errors.New("Persistence provider cannot be nil")
	}

	if s.persist, err = persistence.New(s.config.Persistence); err != nil {
		return nil, err
	}

	var persisRetained persistTypes.Retained

	persisRetained, _ = s.persist.Retained()

	tConfig := &topicsTypes.MemConfig{
		Name:    "mem",
		Stat:    s.sysTree.Topics(),
		Persist: persisRetained,
	}
	if s.topicsMgr, err = topics.New(tConfig); err != nil {
		return nil, err
	}

	mConfig := sessions.Config{
		TopicsMgr:       s.topicsMgr,
		ConnectTimeout:  s.config.ConnectTimeout,
		Persist:         s.persist,
		AllowDuplicates: s.config.AllowDuplicates,
		OnDuplicate:     s.config.OnDuplicate,
		OfflineQoS0:     s.config.OfflineQoS0,
	}
	mConfig.Metric.Packets = s.sysTree.Metric().Packets()
	mConfig.Metric.Session = s.sysTree.Session()
	mConfig.Metric.Sessions = s.sysTree.Sessions()

	if s.sessionsMgr, err = sessions.New(mConfig); err != nil {
		return nil, err
	}

	return s, nil
}

// ListenAndServe configures listener according to provided config
// This is non blocking function. It returns nil if listener started
// to run or error if any happened during configuration.
// Listener status reported over ListenerStatus callback in server configuration
func (s *implementation) ListenAndServe(config listener.Config) error {
	var l listener.Provider
	var err error

	internalConfig := listener.InternalConfig{
		Metric:           s.sysTree.Metric().Bytes(),
		HandleConnection: s.handleConnection,
	}

	switch c := config.(type) {
	case *listener.ConfigTCP:
		c.InternalConfig = internalConfig
		c.Log = s.log.Prod.Named("tcp").Named(strconv.Itoa(c.Port))
		l, err = listener.NewTCP(c)
	case *listener.ConfigWS:
		c.InternalConfig = internalConfig
		c.Log = s.log.Prod.Named("ws").Named(strconv.Itoa(c.Port))
		l, err = listener.NewWS(c)
	default:
		return errors.New("Invalid listener type")
	}

	if err != nil {
		return err
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	if _, ok := s.listeners.list[l.Port()]; ok {
		l.Close() // nolint: errcheck
		return errors.New("Already exists")
	}

	s.listeners.list[l.Port()] = l
	s.listeners.wg.Add(1)
	go func() {
		defer s.listeners.wg.Done()

		if s.config.ListenerStatus != nil {
			s.config.ListenerStatus(":"+strconv.Itoa(l.Port()), "started")
		}

		e := l.Serve()

		if s.config.ListenerStatus != nil {
			status := "stopped"
			if err != nil {
				status = e.Error()
			}
			s.config.ListenerStatus(":"+strconv.Itoa(l.Port()), status)
		}
	}()

	return nil
}

// Close terminates the server by shutting down all the client connections and closing
// the listener. It will, as best it can, clean up after itself.
func (s *implementation) Close() error {
	// By closing the quit channel, we are telling the server to stop accepting new
	// connection.
	s.onClose.Do(func() {
		close(s.quit)

		defer s.lock.Unlock()
		s.lock.Lock()

		// We then close all net.Listener, which will force Accept() to return if it's
		// blocked waiting for new connections.
		for _, l := range s.listeners.list {
			if err := l.Close(); err != nil {
				s.log.Prod.Error(err.Error())
			}
		}

		// Wait all of listeners has finished
		s.listeners.wg.Wait()

		for port := range s.listeners.list {
			delete(s.listeners.list, port)
		}

		if s.sessionsMgr != nil {
			if s.persist != nil {
				s.sessionsMgr.Shutdown() // nolint: errcheck, gas
			}
		}

		if s.topicsMgr != nil {
			s.topicsMgr.Close() // nolint: errcheck, gas
		}
	})

	return nil
}

// handleConnection is for the broker to handle an incoming connection from a client
func (s *implementation) handleConnection(c types.Conn, auth *auth.Manager) {
	if c == nil {
		s.log.Prod.Error("Invalid connection type")
		return
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

	c.SetReadDeadline(time.Now().Add(time.Second * time.Duration(s.config.ConnectTimeout))) // nolint: errcheck, gas

	var req message.Provider

	var buf []byte
	if buf, err = routines.GetMessageBuffer(c); err != nil {
		s.log.Prod.Error("Couldn't get CONNECT message", zap.Error(err))
		return
	}

	if req, _, err = message.Decode(message.ProtocolV50, buf); err != nil {
		s.log.Prod.Warn("Couldn't decode message", zap.Error(err))

		if _, ok := err.(message.ReasonCode); ok {
			if req != nil {
				s.sysTree.Metric().Packets().Received(req.Type())
			}
		}
	} else {
		switch r := req.(type) {
		case *message.ConnectMessage:
			m, _ := message.NewMessage(req.Version(), message.CONNACK)
			resp, _ := m.(*message.ConnAckMessage)

			s.handleConnectionPermission(r, resp, auth)

			s.sessionsMgr.Start(r, resp, c)
		default:
			s.log.Prod.Error("Unexpected message type",
				zap.String("expected", "CONNECT"),
				zap.String("received", r.Type().Name()))
		}
	}
}

func (s *implementation) handleConnectionPermission(req *message.ConnectMessage, resp *message.ConnAckMessage, auth *auth.Manager) {
	user, pass := req.Credentials()

	var reason message.ReasonCode

	if len(user) > 0 {
		if err := auth.Password(string(user), string(pass)); err == nil {
			reason = message.CodeSuccess
		} else {
			reason = message.CodeRefusedBadUsernameOrPassword
			if req.Version() == message.ProtocolV50 {
				reason = message.CodeBadUserOrPassword
			}
		}
	} else {
		if s.config.Anonymous {
			reason = message.CodeSuccess
		} else {
			reason = message.CodeRefusedBadUsernameOrPassword
			if req.Version() == message.ProtocolV50 {
				reason = message.CodeBadUserOrPassword
			}
		}
	}

	if req.KeepAlive() == 0 {
		req.SetKeepAlive(uint16(s.config.KeepAlive))

		// set assigned keep alive for V5.0
	}

	resp.SetReturnCode(reason) // nolint: errcheck
}
