package surgemq

import (
	"errors"
	"strconv"
	"sync"

	"regexp"

	"time"

	"github.com/pborman/uuid"
	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/clients"
	"github.com/troian/surgemq/configuration"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence"
	persistTypes "github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics"
	"github.com/troian/surgemq/topics/types"
	"github.com/troian/surgemq/transport"
	"github.com/troian/surgemq/types"
)

var (
	// nolint: megacheck
	nodeNameRegexp = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
)

var (
	// ErrInvalidNodeName node name does not follow requirements
	ErrInvalidNodeName = errors.New("node name is invalid")
)

// ServerConfig configuration of the MQTT server
type ServerConfig struct {
	// Configuration of persistence provider
	Persistence persistTypes.ProviderConfig

	// OnDuplicate notify if there is attempt connect client with id that already exists and active
	// If not not set than defaults to mock function
	OnDuplicate func(string, bool)

	// TransportStatus user provided callback to track transport status
	// If not set than defaults to mock function
	TransportStatus func(id string, status string)

	// ConnectTimeout The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// KeepAlive The number of seconds to keep the connection live if there's no data.
	// If not set then defaults to 5 minutes.
	KeepAlive int

	// SystreeUpdateInterval
	SystreeUpdateInterval time.Duration

	// NodeName
	NodeName string

	// Authenticator is the authenticator used to check username and password sent
	// in the CONNECT message. If not set then defaults to "mockSuccess".
	Authenticators string

	// AllowedVersions what protocol version server will handle
	// If not set than defaults to 0x3 and 0x04
	AllowedVersions map[message.ProtocolVersion]bool

	// AllowOverlappingSubscriptions tells server how to handle overlapping subscriptions from within one client
	// if true server will send only one publish with max subscribed QoS even there are n subscriptions
	// if false server will send as many publishes as amount of subscriptions matching publish topic exists
	// If not set than default is false
	AllowOverlappingSubscriptions bool

	// RewriteNodeName
	RewriteNodeName bool

	// OfflineQoS0 tell server to either persist (true) or not persist (false) QoS 0 messages for non-clean sessions
	// If not set than default is false
	OfflineQoS0 bool

	// AllowDuplicates Either allow or deny replacing of existing session if there new client with same clientID
	// If not set than default is false
	AllowDuplicates bool

	WithSystree bool
}

// NewServerConfig with default values. It's highly recommended to use that function to allocate config
// rather than directly ServerConfig structure
func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Authenticators:                "mockSuccess",
		Persistence:                   persistTypes.MemConfig{},
		OnDuplicate:                   func(string, bool) {},
		OfflineQoS0:                   false,
		AllowDuplicates:               false,
		AllowOverlappingSubscriptions: true,
		RewriteNodeName:               false,
		WithSystree:                   true,
		SystreeUpdateInterval:         5,
		KeepAlive:                     types.DefaultKeepAlive,
		ConnectTimeout:                types.DefaultConnectTimeout,
		TransportStatus:               func(id string, status string) {},
		AllowedVersions: map[message.ProtocolVersion]bool{
			message.ProtocolV31:  true,
			message.ProtocolV311: true,
		},
	}
}

// Server server API
type Server interface {
	// ListenAndServe configures transport according to provided config
	// This is non blocking function. It returns nil if listener started
	// or error if any happened during configuration.
	// Transport status reported over TransportStatus callback in server configuration
	ListenAndServe(interface{}) error

	// Close terminates the server by shutting down all the client connections and closing
	// configured listeners. It does full clean up of the resources and
	Close() error
}

// server is a library implementation of the MQTT server that, as best it can, complies
// with the MQTT 3.1/3.1.1 and 5.0 specs.
type server struct {
	config *ServerConfig
	// authMgr is the authentication manager that we are going to use for authenticating
	// incoming connections
	authMgr *auth.Manager

	// sessionsMgr is the sessions manager for keeping track of the sessions
	sessionsMgr *clients.Manager

	log types.LogInterface

	// topicsMgr is the topics manager for keeping track of subscriptions
	topicsMgr topicsTypes.Provider

	persist persistTypes.Provider

	sysTree systree.Provider

	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit chan struct{}

	lock sync.Mutex

	onClose sync.Once

	transports struct {
		list map[int]transport.Provider
		wg   sync.WaitGroup
	}

	systree struct {
		done      chan bool
		wgStarted sync.WaitGroup
		wgStopped sync.WaitGroup
		timer     *time.Ticker
	}

	// nodes cluster nodes
	//nodes map[string]subscriber.Provider
}

// NewServer allocate server object
func NewServer(config *ServerConfig) (Server, error) {
	s := &server{}

	if config.NodeName != "" {
		if !nodeNameRegexp.MatchString(config.NodeName) {
			return nil, ErrInvalidNodeName
		}
	}

	s.config = config

	s.log.Prod = configuration.GetProdLogger().Named("server")
	s.log.Dev = configuration.GetDevLogger().Named("server")

	s.quit = make(chan struct{})
	s.transports.list = make(map[int]transport.Provider)

	var err error
	if s.authMgr, err = auth.NewManager(s.config.Authenticators); err != nil {
		return nil, err
	}

	if s.config.Persistence == nil {
		return nil, errors.New("Persistence provider cannot be nil")
	}

	if s.persist, err = persistence.New(s.config.Persistence); err != nil {
		return nil, err
	}

	var systemPersistence persistTypes.System
	var systemState *persistTypes.SystemState

	if systemPersistence, err = s.persist.System(); err != nil {
		return nil, err
	}

	if systemState, err = systemPersistence.GetInfo(); err != nil {
		return nil, err
	}

	generateNodeID := func() string {
		return uuid.New() + "@surgemq.io"
	}

	if systemState.NodeName == "" || s.config.RewriteNodeName {
		if s.config.NodeName == "" {
			s.config.NodeName = generateNodeID()
		}

		systemState.NodeName = s.config.NodeName
	} else {
		s.config.NodeName = systemState.NodeName
	}

	if err = systemPersistence.SetInfo(systemState); err != nil {
		return nil, err
	}

	var persisRetained persistTypes.Retained
	var retains []types.RetainObject
	var dynPublishes []systree.DynamicValue

	if s.sysTree, retains, dynPublishes, err = systree.NewTree("$SYS/servers/" + s.config.NodeName); err != nil {
		return nil, err
	}

	persisRetained, _ = s.persist.Retained()

	tConfig := topicsTypes.NewMemConfig()

	tConfig.Stat = s.sysTree.Topics()
	tConfig.Persist = persisRetained
	tConfig.AllowOverlappingSubscriptions = config.AllowOverlappingSubscriptions

	if s.topicsMgr, err = topics.New(tConfig); err != nil {
		return nil, err
	}

	if s.config.WithSystree {
		s.sysTree.SetCallbacks(s.topicsMgr)

		for _, o := range retains {
			if err = s.topicsMgr.Retain(o); err != nil {
				return nil, err
			}
		}

		if s.config.SystreeUpdateInterval > 0 {
			s.systree.wgStarted.Add(1)
			s.systree.wgStopped.Add(1)
			go s.systreeUpdater(dynPublishes, s.config.SystreeUpdateInterval*time.Second)
			s.systree.wgStarted.Wait()
		}
	}

	mConfig := &clients.Config{
		TopicsMgr:        s.topicsMgr,
		ConnectTimeout:   s.config.ConnectTimeout,
		Persist:          s.persist,
		AllowReplace:     s.config.AllowDuplicates,
		OnReplaceAttempt: s.config.OnDuplicate,
		OfflineQoS0:      s.config.OfflineQoS0,
		Systree:          s.sysTree,
		NodeName:         s.config.NodeName,
	}

	if s.sessionsMgr, err = clients.NewManager(mConfig); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) ListenAndServe(config interface{}) error {
	var l transport.Provider
	var err error

	internalConfig := transport.InternalConfig{
		Metric:          s.sysTree.Metric(),
		Sessions:        s.sessionsMgr,
		ConnectTimeout:  s.config.ConnectTimeout,
		KeepAlive:       s.config.KeepAlive,
		AllowedVersions: s.config.AllowedVersions,
	}

	switch c := config.(type) {
	case *transport.ConfigTCP:
		l, err = transport.NewTCP(c, &internalConfig)
	case *transport.ConfigWS:
		l, err = transport.NewWS(c, &internalConfig)
	default:
		return errors.New("Invalid listener type")
	}

	if err != nil {
		return err
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	if _, ok := s.transports.list[l.Port()]; ok {
		l.Close() // nolint: errcheck
		return errors.New("Already exists")
	}

	s.transports.list[l.Port()] = l
	s.transports.wg.Add(1)
	go func() {
		defer s.transports.wg.Done()

		s.config.TransportStatus(":"+strconv.Itoa(l.Port()), "started")

		status := "stopped"
		if e := l.Serve(); e != nil {
			status = e.Error()
		}
		s.config.TransportStatus(":"+strconv.Itoa(l.Port()), status)
	}()

	return nil
}

func (s *server) Close() error {
	// By closing the quit channel, we are telling the server to stop accepting new
	// connection.
	s.onClose.Do(func() {
		close(s.quit)

		defer s.lock.Unlock()
		s.lock.Lock()

		// shutdown systree updater
		if s.systree.timer != nil {
			s.systree.timer.Stop()
			s.systree.done <- true
			s.systree.wgStopped.Wait()
			close(s.systree.done)
		}

		// We then close all net.Listener, which will force Accept() to return if it's
		// blocked waiting for new connections.
		for _, l := range s.transports.list {
			if err := l.Close(); err != nil {
				s.log.Prod.Error(err.Error())
			}
		}

		// Wait all of listeners has finished
		s.transports.wg.Wait()

		for port := range s.transports.list {
			delete(s.transports.list, port)
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

func (s *server) systreeUpdater(publishes []systree.DynamicValue, period time.Duration) {
	defer s.systree.wgStopped.Done()

	s.systree.done = make(chan bool)
	s.systree.timer = time.NewTicker(period)
	s.systree.wgStarted.Done()

	for {
		select {
		case <-s.systree.timer.C:
			for _, m := range publishes {
				_m := m.Publish()
				_msg, _ := message.NewMessage(message.ProtocolV311, message.PUBLISH)
				msg, _ := _msg.(*message.PublishMessage)

				msg.SetPayload(_m.Payload())
				msg.SetTopic(_m.Topic()) // nolint: errcheck
				msg.SetQoS(_m.QoS())     // nolint: errcheck
				s.topicsMgr.Publish(msg) // nolint: errcheck
			}
		case <-s.systree.done:
			return
		}
	}
}
