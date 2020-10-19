package server

import (
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/VolantMQ/vlapi/vlpersistence"
	"github.com/VolantMQ/vlapi/vlplugin"
	"github.com/VolantMQ/vlapi/vlsubscriber"
	"github.com/VolantMQ/vlapi/vltypes"
	"github.com/troian/healthcheck"
	"go.uber.org/zap"

	"github.com/VolantMQ/volantmq/clients"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/metrics"
	"github.com/VolantMQ/volantmq/topics"
	topicsTypes "github.com/VolantMQ/volantmq/topics/types"
	"github.com/VolantMQ/volantmq/transport"
	"github.com/VolantMQ/volantmq/types"
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
	// ErrInvalidListenerType invalid listener type
	ErrInvalidListenerType = errors.New("invalid listener type")
	// ErrTransportAlreadyExists transport already exists
	ErrTransportAlreadyExists = errors.New("transport already exists")
	// ErrInconsistentPersistenceProvider persistence provider is nil
	ErrInconsistentPersistenceProvider = errors.New("persistence provider cannot be nil")
)

// Config configuration of the MQTT server
type Config struct {
	MQTT     configuration.MqttConfig
	Acceptor configuration.AcceptorConfig

	// Configuration of persistence provider
	Persistence vlpersistence.IFace

	// OnDuplicate notify if there is attempt connect client with id that already exists and active
	// If not not set than defaults to mock function
	OnDuplicate func(string, bool)

	// TransportStatus user provided callback to track transport status
	// If not set than defaults to mock function
	TransportStatus func(id string, status string)
	Health          healthcheck.Checks
	Metrics         metrics.IFace
	NodeName        string
	Version         string
	BuildTimestamp  string
}

// Server server API
type Server interface {
	// ListenAndServe configures transport according to provided config
	// This is non blocking function. It returns nil if listener started
	// or error if any happened during configuration.
	// Transport status reported over TransportStatus callback in server configuration
	ListenAndServe(interface{}) error

	// Shutdown terminates the server by shutting down all the client connections and closing
	// configured listeners. It does full clean up of the resources and
	Shutdown() error

	vlplugin.Messaging
}

// server is a library implementation of the MQTT server that, as best it can, complies
// with the MQTT 3.1/3.1.1 and 5.0 specs.
type server struct {
	Config
	sessionsMgr *clients.Manager
	log         *zap.SugaredLogger
	topicsMgr   topicsTypes.Provider
	quit        chan struct{}
	lock        sync.Mutex
	onClose     sync.Once
	acceptPool  types.Pool
	transports  struct {
		list map[string]transport.Provider
		wg   sync.WaitGroup
	}
}

var _ vlplugin.Messaging = (*server)(nil)

// NewServer allocate server object
func NewServer(config Config) (Server, error) {
	s := &server{
		Config: config,
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

	if s.Persistence == nil {
		return nil, ErrInconsistentPersistenceProvider
	}

	var persisRetained vlpersistence.Retained

	persisRetained, _ = s.Persistence.Retained()

	topicsConfig := topicsTypes.NewMemConfig()

	topicsConfig.MetricsPackets = s.Metrics.Packets()
	topicsConfig.MetricsSubs = s.Metrics.Subs()
	topicsConfig.Persist = persisRetained
	topicsConfig.OverlappingSubscriptions = s.MQTT.Options.SubsOverlap

	if s.topicsMgr, err = topics.New(topicsConfig); err != nil {
		s.log.Errorf("cannot create topics")
		return nil, err
	}

	s.acceptPool = types.NewPool(s.Config.Acceptor.MaxIncoming, 1, s.Config.Acceptor.PreSpawn)

	mConfig := &clients.Config{
		MqttConfig:       s.MQTT,
		TopicsMgr:        s.topicsMgr,
		Persist:          s.Persistence,
		Metrics:          s.Metrics,
		OnReplaceAttempt: s.OnDuplicate,
		NodeName:         s.NodeName,
	}

	if s.sessionsMgr, err = clients.NewManager(mConfig); err != nil {
		s.log.Errorf("cannot create client manager")
		return nil, err
	}

	return s, nil
}

// GetSubscriber ...
func (s *server) GetSubscriber(id string) (vlsubscriber.IFace, error) {
	return s.sessionsMgr.GetSubscriber(id)
}

func (s *server) Publish(vl interface{}) error {
	return s.topicsMgr.Publish(vl)
}

func (s *server) Retain(rt vltypes.RetainObject) error {
	return s.topicsMgr.Retain(rt)
}

// ListenAndServe start listener
func (s *server) ListenAndServe(config interface{}) error {
	var l transport.Provider

	var err error

	internalConfig := transport.InternalConfig{
		Handler:    s.sessionsMgr,
		AcceptPool: s.acceptPool,
		Metrics:    s.Metrics.Bytes(),
	}

	switch c := config.(type) {
	case *transport.ConfigTCP:
		l, err = transport.NewTCP(c, &internalConfig)
	case *transport.ConfigWS:
		l, err = transport.NewWS(c, &internalConfig)
	default:
		return ErrInvalidListenerType
	}

	if err != nil {
		return err
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	if _, ok := s.transports.list[l.Port()]; ok {
		_ = l.Close()
		return ErrTransportAlreadyExists
	}

	s.transports.list[l.Port()] = l
	s.transports.wg.Add(1)
	go func() {
		defer s.transports.wg.Done()

		s.TransportStatus(":"+l.Port(), "started")

		status := "stopped"

		_ = s.Health.AddReadinessCheck("listener:"+l.Port(), func() error {
			if e := l.Ready(); e != nil {
				return e
			}

			return healthcheck.TCPDialCheck(":"+l.Port(), 1*time.Second)()
		})

		_ = s.Health.AddLivenessCheck("listener:"+l.Port(), func() error {
			if e := l.Alive(); e != nil {
				return e
			}

			return healthcheck.TCPDialCheck(":"+l.Port(), 1*time.Second)()
		})

		if e := l.Serve(); e != nil {
			status = e.Error()
		}

		s.TransportStatus(":"+l.Port(), status)
	}()

	return nil
}

// Shutdown server
func (s *server) Shutdown() error {
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

		_ = s.sessionsMgr.Stop()

		if err := s.sessionsMgr.Shutdown(); err != nil {
			s.log.Error("stop session manager", zap.Error(err))
		}

		if err := s.Metrics.Shutdown(); err != nil {
			s.log.Error("stop metrics", zap.Error(err))
		}

		if err := s.topicsMgr.Shutdown(); err != nil {
			s.log.Error("stop topics manager manager", zap.Error(err))
		}

		_ = s.acceptPool.Close()
	})

	return nil
}
