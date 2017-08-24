package clients

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/configuration"
	"github.com/troian/surgemq/packet"
	"github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/routines"
	"github.com/troian/surgemq/subscriber"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics/types"
	"go.uber.org/zap"
	"golang.org/x/sync/syncmap"
)

var (
	// ErrReplaceNotAllowed case when new client with existing ID connected
	ErrReplaceNotAllowed = errors.New("duplicate not allowed")
)

// Config manager configuration
type Config struct {
	// Topics manager for all the client subscriptions
	TopicsMgr topicsTypes.Provider

	Persist persistenceTypes.Provider

	Systree systree.Provider

	// OnReplaceAttempt If requested we notify if there is attempt to dup session
	OnReplaceAttempt func(string, bool)

	NodeName string

	// The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// The number of seconds to keep the connection live if there's no data.
	// If not set then defaults to 5 minutes.
	KeepAlive int

	// AllowReplace Either allow or deny replacing of existing session if there new client with same clientID
	AllowReplace bool

	OfflineQoS0 bool
}

// Manager clients manager
type Manager struct {
	systree          systree.Provider
	persistence      persistenceTypes.Sessions
	topics           topicsTypes.SubscriberInterface
	onReplaceAttempt func(string, bool)
	log              *zap.Logger
	quit             chan struct{}
	sessionsCount    sync.WaitGroup
	sessions         syncmap.Map
	subscribers      syncmap.Map
	allowReplace     bool
	offlineQoS0      bool
}

// StartConfig used to configure session after connection is created
type StartConfig struct {
	Req  *packet.Connect
	Resp *packet.ConnAck
	Conn net.Conn
	Auth auth.SessionPermissions
}

// NewManager create new clients manager
func NewManager(c *Config) (*Manager, error) {
	m := &Manager{
		systree:          c.Systree,
		topics:           c.TopicsMgr,
		onReplaceAttempt: c.OnReplaceAttempt,
		allowReplace:     c.AllowReplace,
		offlineQoS0:      c.OfflineQoS0,
		quit:             make(chan struct{}),
		//sessions:         make(map[string]*session),
		//subscribers: make(map[string]subscriber.ConnectionProvider),
		log: configuration.GetProdLogger().Named("sessions"),
	}

	m.persistence, _ = c.Persist.Sessions()

	m.log.Info("Loading sessions. Might take a while")
	// load sessions for fill systree
	// those sessions having either will delay or expire are created with and timer started
	if err := m.loadSessions(); err != nil {
		return nil, err
	}

	if err := m.loadSubscribers(); err != nil {
		return nil, err
	}

	m.log.Info("Sessions loaded")

	m.persistence.StatesWipe()        // nolint: errcheck
	m.persistence.SubscriptionsWipe() // nolint: errcheck

	return m, nil
}

// Shutdown clients manager
// gracefully shutdown by stopping all active sessions and persist states
func (m *Manager) Shutdown() error {
	select {
	case <-m.quit:
		return errors.New("already stopped")
	default:
		close(m.quit)
	}

	m.sessions.Range(func(k, v interface{}) bool {
		ses := v.(*session)
		state := ses.stop(packet.CodeServerShuttingDown)
		m.persistence.StateStore([]byte(k.(string)), state) // nolint: errcheck
		return true
	})

	m.sessionsCount.Wait()

	m.storeSubscribers() // nolint: errcheck

	return nil
}

// NewSession create new session with provided established connection
// This is god function. Might be try split it
func (m *Manager) NewSession(config *StartConfig) {
	var id string
	var ses *session
	var sub subscriber.ConnectionProvider
	var err error
	idGenerated := false
	sessionPresent := false
	username, _ := config.Req.Credentials()

	sesProperties := newProperties()

	defer func() {
		if err != nil {
			var reason packet.ReasonCode
			switch config.Req.Version() {
			case packet.ProtocolV50:
				reason = packet.CodeUnspecifiedError
			default:
				reason = packet.CodeRefusedServerUnavailable
			}
			config.Resp.SetReturnCode(reason) // nolint: errcheck
		} else {
			config.Resp.SetSessionPresent(sessionPresent)

			if idGenerated {
				config.Resp.PropertySet(packet.PropertyAssignedClientIdentifier, id) // nolint: errcheck
			}
		}

		if err = routines.WriteMessage(config.Conn, config.Resp); err != nil {
			m.log.Error("Couldn't write CONNACK", zap.String("ClientID", id), zap.Error(err))
		} else {
			if ses != nil {
				ses.start()
				m.systree.Clients().Connected(
					id,
					&systree.ClientConnectStatus{
						Address:           config.Conn.RemoteAddr().String(),
						Username:          string(username),
						Timestamp:         time.Now().Format(time.RFC3339),
						ReceiveMaximum:    uint32(sesProperties.ReceiveMaximum),
						MaximumPacketSize: sesProperties.MaximumPacketSize,
						KeepAlive:         config.Req.KeepAlive(),
						GeneratedID:       idGenerated,
						CleanSession:      config.Req.CleanStart(),
						SessionPresent:    sessionPresent,
						Protocol:          config.Req.Version(),
						ConnAckCode:       config.Resp.ReturnCode(),
					})
			}
		}

		if ses != nil {
			ses.release()
		}
	}()

	// check first if server is not about to shutdown
	// if so just give reject and exit
	select {
	case <-m.quit:
		var reason packet.ReasonCode
		switch config.Req.Version() {
		case packet.ProtocolV50:
			reason = packet.CodeServerShuttingDown
			// TODO: if cluster route client to another node
		default:
			reason = packet.CodeRefusedServerUnavailable
		}
		config.Resp.SetReturnCode(reason) // nolint: errcheck
	default:
	}

	// if response has return code differs from CodeSuccess return from this point
	// and send connack in deferred statement
	if config.Resp.ReturnCode() != packet.CodeSuccess {
		return
	}

	// client might come with empty client id
	if id = string(config.Req.ClientID()); len(id) == 0 {
		id = m.genClientID()
		idGenerated = true
	}

	config.Req.PropertyForEach(func(id packet.PropertyID, val interface{}) { // nolint: errcheck
		switch id {
		case packet.PropertySessionExpiryInterval:
			v := time.Duration(val.(uint32))
			sesProperties.ExpireIn = &v
		case packet.PropertyWillDelayInterval:
			sesProperties.WillDelay = time.Duration(val.(uint32))
		case packet.PropertyReceiveMaximum:
			sesProperties.ReceiveMaximum = val.(uint16)
		case packet.PropertyMaximumPacketSize:
			sesProperties.MaximumPacketSize = val.(uint32)
		case packet.PropertyTopicAliasMaximum:
			sesProperties.TopicAliasMaximum = val.(uint16)
		case packet.PropertyRequestProblemInfo:
			sesProperties.RequestProblemInfo = val.(bool)
		case packet.PropertyRequestResponseInfo:
			sesProperties.RequestResponse = val.(bool)
		case packet.PropertyUserProperty:
			sesProperties.UserProperties = val
		case packet.PropertyAuthMethod:
			sesProperties.AuthMethod = val.(string)
		case packet.PropertyAuthData:
			sesProperties.AuthData = val.([]byte)
		}
	})

	if ss, ok := m.sessions.Load(id); ok {
		ses = ss.(*session)
		ses.acquire()
	}

	if ses != nil && !ses.toOnline() {
		if !m.allowReplace {
			// duplicate prohibited. send identifier rejected
			var reason packet.ReasonCode
			switch config.Req.Version() {
			case packet.ProtocolV50:
				reason = packet.CodeInvalidClientID
			default:
				reason = packet.CodeRefusedIdentifierRejected
			}

			config.Resp.SetReturnCode(reason) // nolint: errcheck
			err = ErrReplaceNotAllowed
			m.onReplaceAttempt(id, false)
			ses = nil
			return
		}
		// replace allowed stop current session
		ses.stop(packet.CodeSessionTakenOver)
		ses.release()
		m.onReplaceAttempt(id, true)
		ses = nil
	}

	var persistedMessages *persistenceTypes.SessionMessages

	sub, persistedMessages, sessionPresent = m.getSubscriber(id, config.Req.CleanStart(), config.Req.Version())

	expireInterval := "absent"
	if sesProperties.ExpireIn != nil {
		expireInterval = strconv.FormatUint(uint64(*sesProperties.ExpireIn), 10)
	}

	if !sessionPresent {
		state := &systree.SessionCreatedStatus{
			ExpiryInterval: expireInterval,
			WillDelay:      strconv.FormatUint(uint64(sesProperties.WillDelay), 10),
			Timestamp:      time.Now().Format(time.RFC3339),
			Clean:          config.Req.CleanStart(),
		}

		m.systree.Sessions().Created(id, state)
	}

	var willMsg *packet.Publish
	if willTopic, willPayload, willQoS, willRetain, will := config.Req.Will(); will {
		_m, _ := packet.NewMessage(config.Req.Version(), packet.PUBLISH)
		willMsg = _m.(*packet.Publish)
		willMsg.SetQoS(willQoS)     // nolint: errcheck
		willMsg.SetTopic(willTopic) // nolint: errcheck
		willMsg.SetPayload(willPayload)
		willMsg.SetRetain(willRetain)
	}

	if ses == nil {
		ses, err = newSession(&sessionConfig{
			id:           id,
			onPersist:    m.onSessionPersist,
			onClose:      m.onSessionClose,
			onDisconnect: m.onClientDisconnect,
			messenger:    m.topics,
			clean:        config.Req.CleanStart(),
		})
		if err == nil {
			ses.acquire()
			m.sessions.Store(id, ses)
			m.sessionsCount.Add(1)
		}
	}

	if ses != nil {
		ses.configure(&setupConfig{
			subscriber: sub,
			will:       willMsg,
			expireIn:   sesProperties.ExpireIn,
			willDelay:  sesProperties.WillDelay,
		}, false)

		err = ses.allocConnection(&connectionConfig{
			username:  string(username),
			state:     persistedMessages,
			auth:      config.Auth,
			metric:    m.systree.Metric(),
			conn:      config.Conn,
			keepAlive: config.Req.KeepAlive(),
			sendQuota: sesProperties.ReceiveMaximum,
			version:   config.Req.Version(),
		})

		if err == nil && persistedMessages != nil {
			m.persistence.MessagesWipe([]byte(id)) // nolint: errcheck
		}
	}
}

func (m *Manager) getSubscriber(id string, clean bool, v packet.ProtocolVersion) (subscriber.ConnectionProvider, *persistenceTypes.SessionMessages, bool) {
	var sub subscriber.ConnectionProvider
	var state *persistenceTypes.SessionMessages
	present := false

	if clean {
		if sb, ok := m.subscribers.Load(id); ok {
			sub = sb.(subscriber.ConnectionProvider)
			sub.Offline(true)
			m.subscribers.Delete(id)
		}
		m.persistence.Delete([]byte(id)) // nolint: errcheck
	} else {
		var err error
		if state, err = m.persistence.MessagesLoad([]byte(id)); err != nil && err != persistenceTypes.ErrNotFound {
			m.log.Error("Couldn't load session state", zap.String("ClientID", id), zap.Error(err))
		} else if err == nil {
			present = true
			m.persistence.Delete([]byte(id)) // nolint: errcheck
		}
	}

	if sb, ok := m.subscribers.Load(id); !ok {
		sub = subscriber.New(&subscriber.Config{
			ID:               id,
			Topics:           m.topics,
			OnOfflinePublish: m.onPublish,
			OfflineQoS0:      m.offlineQoS0,
			Version:          v,
		})

		m.subscribers.Store(id, sub)
	} else {
		sub = sb.(subscriber.ConnectionProvider)
		present = true
	}

	return sub, state, present
}

func (m *Manager) genClientID() string {
	b := make([]byte, 15)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}

	return base64.URLEncoding.EncodeToString(b)
}

func (m *Manager) onSessionPersist(id string, state *persistenceTypes.SessionMessages) {
	m.persistence.MessagesStore([]byte(id), state) // nolint: errcheck
}

func (m *Manager) onClientDisconnect(id string, clean bool, reason packet.ReasonCode) {
	m.systree.Clients().Disconnected(id, reason, clean)
}

func (m *Manager) onSessionClose(id string, reason exitReason) {
	if reason != exitReasonKeepSubscriber {
		m.subscribers.Delete(id)
	}

	if reason == exitReasonClean {
		m.persistence.Delete([]byte(id)) // nolint: errcheck
	}

	if reason == exitReasonClean || reason == exitReasonExpired {
		rs := "clean"
		if reason == exitReasonExpired {
			rs = "expired"
		}

		state := &systree.SessionDeletedStatus{
			Timestamp: time.Now().Format(time.RFC3339),
			Reason:    rs,
		}

		m.systree.Sessions().Removed(id, state)
	}

	m.sessions.Delete(id)
	m.sessionsCount.Done()
}

func (m *Manager) loadSessions() error {
	err := m.persistence.StatesIterate(func(id []byte, state *persistenceTypes.SessionState) error {
		status := &systree.SessionCreatedStatus{
			Clean:     false,
			Timestamp: state.Timestamp,
		}

		if state.ExpireIn != nil {
			if state.ExpireIn != nil {
				status.ExpiryInterval = strconv.FormatUint(uint64(*state.ExpireIn), 10)
			}
		}

		if state.Will != nil {
			status.WillDelay = strconv.FormatUint(uint64(state.Will.Delay), 10)
		}

		if state.ExpireIn != nil || state.Will != nil {
			var err error
			var ses *session
			createdAt, _ := time.Parse(time.RFC3339, state.Timestamp)
			ses, err = newSession(&sessionConfig{
				id:           string(id),
				createdAt:    createdAt,
				onPersist:    m.onSessionPersist,
				onClose:      m.onSessionClose,
				onDisconnect: m.onClientDisconnect,
				messenger:    m.topics,
				clean:        false,
			})
			if err == nil {
				m.sessions.Store(string(id), ses)
				m.sessionsCount.Add(1)

				setup := &setupConfig{
					subscriber: nil,
					expireIn:   state.ExpireIn,
				}

				if state.Will != nil {
					msg, _, _ := packet.Decode(state.Version, state.Will.Message)
					willMsg, _ := msg.(*packet.Publish)
					setup.will = willMsg
					setup.willDelay = state.Will.Delay
				}
				ses.configure(setup, true)
			} else {
				return err
			}
		}
		m.systree.Sessions().Created(string(id), status)

		return nil
	})

	return err
}

func (m *Manager) loadSubscribers() error {
	// load sessions owning subscriptions
	type subscriberConfig struct {
		version packet.ProtocolVersion
		topics  subscriber.Subscriptions
	}
	subs := map[string]subscriberConfig{}

	err := m.persistence.SubscriptionsIterate(func(id []byte, data []byte) error {
		subscriptions := subscriber.Subscriptions{}
		offset := 0
		version := packet.ProtocolVersion(data[offset])
		offset++
		remaining := len(data) - 1
		for offset != remaining {
			t, total, e := packet.ReadLPBytes(data[offset:])
			if e != nil {
				return e
			}

			offset += total

			params := &subscriber.SubscriptionParams{}

			params.Requested = packet.SubscriptionOptions(data[offset])
			offset++

			params.ID = binary.BigEndian.Uint32(data[offset:])
			offset += 4
			subscriptions[string(t)] = params
		}

		subs[string(id)] = subscriberConfig{
			version: version,
			topics:  subscriptions,
		}
		return nil
	})

	if err != nil && err != persistenceTypes.ErrNotFound {
		return err
	}

	for id, t := range subs {
		sub := subscriber.New(
			&subscriber.Config{
				ID:               id,
				Topics:           m.topics,
				OnOfflinePublish: m.onPublish,
				OfflineQoS0:      m.offlineQoS0,
				Version:          t.version,
			})

		for topic, ops := range t.topics {
			if _, _, err = sub.Subscribe(topic, ops); err != nil {
				m.log.Error("Couldn't subscribe", zap.Error(err))
			}
		}

		m.subscribers.Store(id, sub)
	}

	return nil
}

func (m *Manager) storeSubscribers() error {
	// 4. shutdown and persist subscriptions from non-clean session
	//for id, s := range m.subscribers {
	m.subscribers.Range(func(k, v interface{}) bool {
		id := k.(string)
		s := v.(subscriber.ConnectionProvider)
		s.Offline(true)

		topics := s.Subscriptions()

		// calculate size of the encoded entry
		// consist of:
		//  _ _ _ _ _     _ _ _ _ _ _
		// |_|_|_|_|_|...|_|_|_|_|_|_|
		//  ___ _ _________ _ _______
		//   |  |     |     |    |
		//   |  |     |     |    4 bytes - subscription id
		//   |  |     |     | 1 byte - topic options
		//   |  |     | n bytes - topic
		//   |  | 1 bytes - protocol version
		//   | 2 bytes - length prefix

		size := 0
		for topic := range topics {
			size += 2 + len(topic) + 1 + int(unsafe.Sizeof(uint32(0)))
		}

		buf := make([]byte, size+1)
		offset := 0
		buf[offset] = byte(s.Version())
		offset++

		for s, params := range topics {
			total, _ := packet.WriteLPBytes(buf[offset:], []byte(s))
			offset += total
			buf[offset] = byte(params.Requested)
			offset++
			binary.BigEndian.PutUint32(buf[offset:], params.ID)
			offset += 4
		}

		if err := m.persistence.SubscriptionStore([]byte(id), buf); err != nil {
			m.log.Error("Couldn't persist subscriptions", zap.String("ClientID", id), zap.Error(err))
		}

		return true
	})

	return nil
}

func (m *Manager) onPublish(id string, msg *packet.Publish) {
	msg.SetPacketID(0)
	sz, err := msg.Size()
	if err != nil {
		m.log.Error("Couldn't get message size", zap.String("ClientID", id), zap.Error(err))
		return
	}

	buf := make([]byte, sz)
	if _, err = msg.Encode(buf); err != nil {
		m.log.Error("Couldn't encode message", zap.String("ClientID", id), zap.Error(err))
		return
	}

	if err = m.persistence.MessageStore([]byte(id), buf); err != nil {
		m.log.Error("Couldn't persist message", zap.String("ClientID", id), zap.Error(err))
	}
}
