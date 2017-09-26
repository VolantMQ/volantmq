package volantmq

import (
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/VolantMQ/persistence"
	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/clients"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/topics"
	"github.com/VolantMQ/volantmq/topics/types"
	"github.com/VolantMQ/volantmq/transport"
	"github.com/VolantMQ/volantmq/types"
	"github.com/pborman/uuid"
	"go.uber.org/zap"
)

var (
	// nolint: megacheck
	nodeNameRegexp = regexp.MustCompile(
		"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}" +
			"[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
)

var (
	// ErrInvalidNodeName node name does not follow requirements
	ErrInvalidNodeName = errors.New("node name is invalid")
)

// ServerConfig configuration of the MQTT server
type ServerConfig struct {
	// Configuration of persistence provider
	Persistence persistence.Provider

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
	AllowedVersions map[packet.ProtocolVersion]bool

	// MaxPacketSize
	MaxPacketSize uint32

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

	// WithSystree
	WithSystree bool

	// ForceKeepAlive
	ForceKeepAlive bool
}

// NewServerConfig with default values. It's highly recommended to use that function to allocate config
// rather than directly ServerConfig structure
func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Authenticators:                "mockSuccess",
		Persistence:                   persistence.Default(),
		OnDuplicate:                   func(string, bool) {},
		OfflineQoS0:                   false,
		AllowDuplicates:               false,
		AllowOverlappingSubscriptions: true,
		RewriteNodeName:               false,
		WithSystree:                   true,
		SystreeUpdateInterval:         0,
		KeepAlive:                     types.DefaultKeepAlive,
		ConnectTimeout:                types.DefaultConnectTimeout,
		MaxPacketSize:                 types.DefaultMaxPacketSize,
		TransportStatus:               func(id string, status string) {},
		AllowedVersions: map[packet.ProtocolVersion]bool{
			packet.ProtocolV31:  true,
			packet.ProtocolV311: true,
			packet.ProtocolV50:  true,
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
	*ServerConfig
	authMgr     *auth.Manager
	sessionsMgr *clients.Manager
	log         *zap.Logger
	topicsMgr   topicsTypes.Provider
	sysTree     systree.Provider
	quit        chan struct{}
	lock        sync.Mutex
	onClose     sync.Once
	transports  struct {
		list map[string]transport.Provider
		wg   sync.WaitGroup
	}
	systree struct {
		publishes []systree.DynamicValue
		timer     *time.Timer
	}
}

// NewServer allocate server object
func NewServer(config *ServerConfig) (Server, error) {
	s := &server{
		ServerConfig: config,
	}

	if config.NodeName != "" {
		if !nodeNameRegexp.MatchString(config.NodeName) {
			return nil, ErrInvalidNodeName
		}
	}

	s.log = configuration.GetLogger().Named("server")

	s.quit = make(chan struct{})
	s.transports.list = make(map[string]transport.Provider)

	var err error
	if s.authMgr, err = auth.NewManager(s.Authenticators); err != nil {
		return nil, err
	}

	if s.Persistence == nil {
		return nil, errors.New("persistence provider cannot be nil")
	}

	var systemPersistence persistence.System
	var systemState *persistence.SystemState

	if systemPersistence, err = s.Persistence.System(); err != nil {
		return nil, err
	}

	if systemState, err = systemPersistence.GetInfo(); err != nil {
		return nil, err
	}

	generateNodeID := func() string {
		return uuid.New() + "@volantmq.io"
	}

	if systemState.NodeName == "" || s.RewriteNodeName {
		if s.NodeName == "" {
			s.NodeName = generateNodeID()
		}

		systemState.NodeName = s.NodeName
	} else {
		s.NodeName = systemState.NodeName
	}

	if err = systemPersistence.SetInfo(systemState); err != nil {
		return nil, err
	}

	var persisRetained persistence.Retained
	var retains []types.RetainObject
	//var dynPublishes []systree.DynamicValue

	if s.sysTree, retains, s.systree.publishes, err = systree.NewTree("$SYS/servers/" + s.NodeName); err != nil {
		return nil, err
	}

	persisRetained, _ = s.Persistence.Retained()

	tConfig := topicsTypes.NewMemConfig()

	tConfig.Stat = s.sysTree.Topics()
	tConfig.Persist = persisRetained
	tConfig.AllowOverlappingSubscriptions = config.AllowOverlappingSubscriptions

	if s.topicsMgr, err = topics.New(tConfig); err != nil {
		return nil, err
	}

	if s.WithSystree {
		s.sysTree.SetCallbacks(s.topicsMgr)

		for _, o := range retains {
			if err = s.topicsMgr.Retain(o); err != nil {
				return nil, err
			}
		}

		if s.SystreeUpdateInterval > 0 {
			s.systree.timer = time.AfterFunc(s.SystreeUpdateInterval*time.Second, s.systreeUpdater)
		}
	}

	mConfig := &clients.Config{
		TopicsMgr:                     s.topicsMgr,
		ConnectTimeout:                s.ConnectTimeout,
		Persist:                       s.Persistence,
		Systree:                       s.sysTree,
		AllowReplace:                  s.AllowDuplicates,
		OnReplaceAttempt:              s.OnDuplicate,
		NodeName:                      s.NodeName,
		OfflineQoS0:                   s.OfflineQoS0,
		AvailableRetain:               true,
		AvailableSubscriptionID:       false,
		AvailableSharedSubscription:   false,
		AvailableWildcardSubscription: true,
		TopicAliasMaximum:             0xFFFF,
		ReceiveMax:                    types.DefaultReceiveMax,
		MaxPacketSize:                 types.DefaultMaxPacketSize,
		MaximumQoS:                    packet.QoS2,
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
		ConnectTimeout:  s.ConnectTimeout,
		KeepAlive:       s.KeepAlive,
		AllowedVersions: s.AllowedVersions,
	}

	switch c := config.(type) {
	case *transport.ConfigTCP:
		l, err = transport.NewTCP(c, &internalConfig)
	case *transport.ConfigWS:
		l, err = transport.NewWS(c, &internalConfig)
	default:
		return errors.New("invalid listener type")
	}

	if err != nil {
		return err
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	if _, ok := s.transports.list[l.Port()]; ok {
		l.Close() // nolint: errcheck
		return errors.New("already exists")
	}

	s.transports.list[l.Port()] = l
	s.transports.wg.Add(1)
	go func() {
		defer s.transports.wg.Done()

		s.TransportStatus(":"+l.Port(), "started")

		status := "stopped"
		if e := l.Serve(); e != nil {
			status = e.Error()
		}
		s.TransportStatus(":"+l.Port(), status)
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

		// We then close all net.Listener, which will force Accept() to return if it's
		// blocked waiting for new connections.
		for _, l := range s.transports.list {
			if err := l.Close(); err != nil {
				s.log.Error(err.Error())
			}
		}

		// Wait all of listeners has finished
		s.transports.wg.Wait()

		for port := range s.transports.list {
			delete(s.transports.list, port)
		}

		if s.sessionsMgr != nil {
			if s.Persistence != nil {
				s.sessionsMgr.Shutdown() // nolint: errcheck, gas
			}
		}

		if s.topicsMgr != nil {
			s.topicsMgr.Close() // nolint: errcheck, gas
		}

		// shutdown systree updater
		if s.systree.timer != nil {
			s.systree.timer.Stop()
		}

	})

	return nil
}

func (s *server) systreeUpdater() {
	for _, m := range s.systree.publishes {
		_m := m.Publish()
		_msg, _ := packet.New(packet.ProtocolV311, packet.PUBLISH)
		msg, _ := _msg.(*packet.Publish)

		msg.SetPayload(_m.Payload())
		msg.SetTopic(_m.Topic()) // nolint: errcheck
		msg.SetQoS(_m.QoS())     // nolint: errcheck
		s.topicsMgr.Publish(msg) // nolint: errcheck
	}

	s.systree.timer.Reset(s.SystreeUpdateInterval * time.Second)
}
