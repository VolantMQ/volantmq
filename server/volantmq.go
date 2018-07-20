package server

import (
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/plugin"
	"github.com/VolantMQ/vlapi/plugin/persistence"
	"github.com/VolantMQ/vlapi/subscriber"
	"github.com/VolantMQ/volantmq/clients"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/topics"
	"github.com/VolantMQ/volantmq/topics/types"
	"github.com/VolantMQ/volantmq/transport"
	"github.com/VolantMQ/volantmq/types"
	"github.com/troian/healthcheck"
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

// Config configuration of the MQTT server
type Config struct {
	MQTT configuration.MqttConfig

	// Configuration of persistence provider
	Persistence persistence.IFace

	// OnDuplicate notify if there is attempt connect client with id that already exists and active
	// If not not set than defaults to mock function
	OnDuplicate func(string, bool)

	// TransportStatus user provided callback to track transport status
	// If not set than defaults to mock function
	TransportStatus func(id string, status string)

	Health healthcheck.Checks

	// NodeName
	NodeName string
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
}

// server is a library implementation of the MQTT server that, as best it can, complies
// with the MQTT 3.1/3.1.1 and 5.0 specs.
type server struct {
	Config
	sessionsMgr *clients.Manager
	log         *zap.SugaredLogger
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
		timer     *time.Ticker
		wg        sync.WaitGroup
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

	if err = systemPersistence.SetInfo(systemState); err != nil {
		return nil, err
	}

	var persisRetained persistence.Retained
	var retains []types.RetainObject

	if s.sysTree, retains, s.systree.publishes, err = systree.NewTree("$SYS/servers/" + s.NodeName); err != nil {
		return nil, err
	}

	persisRetained, _ = s.Persistence.Retained()

	topicsConfig := topicsTypes.NewMemConfig()

	topicsConfig.Stat = s.sysTree.Topics()
	topicsConfig.Persist = persisRetained
	topicsConfig.OverlappingSubscriptions = s.MQTT.Options.SubsOverlap

	if s.topicsMgr, err = topics.New(topicsConfig); err != nil {
		return nil, err
	}

	if s.MQTT.Systree.Enabled {
		s.sysTree.SetCallbacks(s.topicsMgr)

		for _, o := range retains {
			if err = s.topicsMgr.Retain(o); err != nil {
				return nil, err
			}
		}

		if s.MQTT.Systree.UpdateInterval > 0 {
			s.systree.timer = time.NewTicker(time.Duration(s.MQTT.Systree.UpdateInterval) * time.Second)
			s.systree.wg.Add(1)
			go s.systreeUpdater()
		}
	}

	mConfig := &clients.Config{
		MqttConfig:       s.MQTT,
		TopicsMgr:        s.topicsMgr,
		Persist:          s.Persistence,
		Systree:          s.sysTree,
		OnReplaceAttempt: s.OnDuplicate,
		NodeName:         s.NodeName,
	}

	if s.sessionsMgr, err = clients.NewManager(mConfig); err != nil {
		return nil, err
	}

	return s, nil
}

// GetSubscriber ...
func (s *server) GetSubscriber(id string) (vlsubscriber.IFace, error) {
	return s.sessionsMgr.GetSubscriber(id)
}

// ListenAndServe start listener
func (s *server) ListenAndServe(config interface{}) error {
	var l transport.Provider

	var err error

	internalConfig := transport.InternalConfig{
		Handler: s.sessionsMgr,
		Metric:  s.sysTree.Metric(),
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

		s.Health.AddReadinessCheck("listener:"+l.Port(), func() error {
			if e := l.Ready(); e != nil {
				return e
			}

			return healthcheck.TCPDialCheck(":"+l.Port(), 1*time.Second)()
		})

		s.Health.AddLivenessCheck("listener:"+l.Port(), func() error {
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

		s.sessionsMgr.Stop() // nolint: errcheck, gas

		// shutdown systree updater
		if s.systree.timer != nil {
			s.systree.timer.Stop()
			s.systree.wg.Wait()
		}

		s.topicsMgr.Stop() // nolint: errcheck, gas

		s.sessionsMgr.Shutdown() // nolint: errcheck, gas

		s.topicsMgr.Shutdown() // nolint: errcheck, gas
	})

	return nil
}

func (s *server) systreeUpdater() {
	defer func() {
		s.systree.wg.Done()
	}()

	select {
	case <-s.systree.timer.C:
		for _, val := range s.systree.publishes {
			p := val.Publish()
			pkt := mqttp.NewPublish(mqttp.ProtocolV311)

			pkt.SetPayload(p.Payload())
			pkt.SetTopic(p.Topic())  // nolint: errcheck
			pkt.SetQoS(p.QoS())      // nolint: errcheck
			s.topicsMgr.Publish(pkt) // nolint: errcheck
		}
	case <-s.quit:
		return
	}
}
