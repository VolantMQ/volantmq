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

	"github.com/VolantMQ/persistence"
	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/connection"
	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/routines"
	"github.com/VolantMQ/volantmq/subscriber"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/topics/types"
	"github.com/VolantMQ/volantmq/types"
	"github.com/troian/easygo/netpoll"
	"go.uber.org/zap"
)

// load sessions owning subscriptions
type subscriberConfig struct {
	version packet.ProtocolVersion
	topics  subscriber.Subscriptions
}

// Config manager configuration
type Config struct {
	TopicsMgr                     topicsTypes.Provider
	Persist                       persistence.Provider
	Systree                       systree.Provider
	OnReplaceAttempt              func(string, bool)
	NodeName                      string
	ConnectTimeout                int
	KeepAlive                     int
	MaxPacketSize                 uint32
	ReceiveMax                    uint16
	TopicAliasMaximum             uint16
	MaximumQoS                    packet.QosType
	AvailableRetain               bool
	AvailableWildcardSubscription bool
	AvailableSubscriptionID       bool
	AvailableSharedSubscription   bool
	OfflineQoS0                   bool
	AllowReplace                  bool
	ForceKeepAlive                bool
}

// Manager clients manager
type Manager struct {
	Config
	persistence   persistence.Sessions
	log           *zap.Logger
	quit          chan struct{}
	sessionsCount sync.WaitGroup
	sessions      sync.Map
	subscribers   sync.Map
	poll          netpoll.EventPoll
}

// StartConfig used to reconfigure session after connection is created
type StartConfig struct {
	Req  *packet.Connect
	Resp *packet.ConnAck
	Conn net.Conn
	Auth auth.SessionPermissions
}

// NewManager create new clients manager
func NewManager(c *Config) (*Manager, error) {
	m := &Manager{
		Config: *c,
		quit:   make(chan struct{}),
		log:    configuration.GetLogger().Named("sessions"),
	}

	m.poll, _ = netpoll.New(nil)
	m.persistence, _ = c.Persist.Sessions()

	var err error

	m.log.Info("Loading sessions. Might take a while")

	// load sessions for fill systree
	// those sessions having either will delay or expire are created with and timer started
	if err = m.loadSessions(); err != nil {
		return nil, err
	}

	//if err = m.loadSubscribers(); err != nil {
	//	return nil, err
	//}

	m.log.Info("Sessions loaded")

	//m.persistence.StatesWipe()        // nolint: errcheck
	//m.persistence.SubscriptionsWipe() // nolint: errcheck

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
		wrap := v.(*sessionWrap)
		state := wrap.s.stop(packet.CodeServerShuttingDown)
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
	var err error
	idGenerated := false

	var systreeConnStatus *systree.ClientConnectStatus

	defer func() {
		if err != nil {
			var reason packet.ReasonCode
			var hasErrReson bool
			if reason, hasErrReson = err.(packet.ReasonCode); !hasErrReson {
				switch config.Req.Version() {
				case packet.ProtocolV50:
					reason = packet.CodeUnspecifiedError
				default:
					reason = packet.CodeRefusedServerUnavailable
				}
			}
			config.Resp.SetReturnCode(reason) // nolint: errcheck
			m.log.Warn("Session create error.", zap.Error(err))
		}

		if err = routines.WriteMessage(config.Conn, config.Resp); err != nil {
			m.log.Error("Couldn't write CONNACK", zap.String("ClientID", id), zap.Error(err))
		} else {
			if ses != nil {
				ses.start()
				m.Systree.Clients().Connected(id, systreeConnStatus)
			}
		}
	}()

	m.checkServerStatus(config.Req.Version(), config.Resp)

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

	if ses, err = m.loadSession(id, config.Req.Version(), config.Resp); err == nil {
		if systreeConnStatus, err = m.configureSession(config, ses, id, idGenerated); err != nil {
			m.sessions.Delete(id)
			m.sessionsCount.Done()
		}
	}
}

func (m *Manager) loadSession(id string, v packet.ProtocolVersion, resp *packet.ConnAck) (*session, error) {
	var err error

	var ses *session
	wrap := m.allocSession(id, time.Now())
	if ss, ok := m.sessions.LoadOrStore(id, wrap); ok {
		// release lock of newly allocated session as lock from old one will be used
		wrap.release()
		// there is some old session exists, check if it has active network connection then stop if so
		// if it is offline (either waiting for expiration to fire or will event or) switch back to online
		// and use this session henceforth
		oldWrap := ss.(*sessionWrap)

		// lock id to prevent other upcoming session make any changes until we done
		oldWrap.acquire()

		old := oldWrap.s

		switch old.setOnline() {
		case swStatusIsOnline:
			// existing session has active network connection
			// exempt it if allowed
			m.OnReplaceAttempt(id, m.AllowReplace)
			if !m.AllowReplace {
				// we do not make any changes to current network connection
				// response to new one with error and release both new & old sessions
				err = packet.CodeRefusedIdentifierRejected
				if v >= packet.ProtocolV50 {
					err = packet.CodeInvalidClientID
				}
				oldWrap.release()
			} else {
				// [MQTT-5]
				// session will be replaced with new connection
				// stop current active connection
				old.stop(packet.CodeSessionTakenOver)
				ses = oldWrap.swap(wrap)
				m.sessions.Store(id, oldWrap)
				m.sessionsCount.Add(1)
			}
		case swStatusSwitched:
			// session has been turned online successfully
			ses = old
		default:
			ses = oldWrap.swap(wrap)
			m.sessions.Store(id, oldWrap)
			m.sessionsCount.Add(1)
		}
	} else {
		ses = wrap.s
		m.sessionsCount.Add(1)
	}

	return ses, err
}

func (m *Manager) checkServerStatus(v packet.ProtocolVersion, resp *packet.ConnAck) {
	// check first if server is not about to shutdown
	// if so just give reject and exit
	select {
	case <-m.quit:
		var reason packet.ReasonCode
		switch v {
		case packet.ProtocolV50:
			reason = packet.CodeServerShuttingDown
			// TODO: if cluster route client to another node
		default:
			reason = packet.CodeRefusedServerUnavailable
		}
		resp.SetReturnCode(reason) // nolint: errcheck
	default:
	}
}

func (m *Manager) allocSession(id string, createdAt time.Time) *sessionWrap {
	wrap := &sessionWrap{
		s: newSession(&sessionPreConfig{
			id:        id,
			createdAt: createdAt,
			messenger: m.TopicsMgr,
			sessionEvents: sessionEvents{
				signalClose:        m.onSessionClose,
				signalDisconnected: m.onDisconnect,
				shutdownSubscriber: m.onSubscriberShutdown,
			},
		})}
	wrap.acquire()
	wrap.s.idLock = &wrap.lock

	return wrap
}

func (m *Manager) getWill(pkt *packet.Connect) *packet.Publish {
	var willPkt *packet.Publish
	if willTopic, willPayload, willQoS, willRetain, will := pkt.Will(); will {
		_m, _ := packet.New(pkt.Version(), packet.PUBLISH)
		willPkt = _m.(*packet.Publish)
		willPkt.Set(willTopic, willPayload, willQoS, willRetain, false) // nolint: errcheck
	}

	return willPkt
}

func (m *Manager) newConnectionPreConfig(config *StartConfig) *connection.PreConfig {
	username, _ := config.Req.Credentials()

	return &connection.PreConfig{
		Username:        	string(username),
		Auth:            	config.Auth,
		Conn:            	config.Conn,
		KeepAlive:       	config.Req.KeepAlive(),
		Version:         	config.Req.Version(),
		Desc:            	netpoll.Must(netpoll.HandleReadOnce(config.Conn)),
		MaxTxPacketSize: 	types.DefaultMaxPacketSize,
		SendQuota:       	types.DefaultReceiveMax,
		PersistedSession:   m.persistence,
		EventPoll:       	m.poll,
		Metric:          	m.Systree.Metric(),
		RetainAvailable: 	m.AvailableRetain,
		OfflineQoS0:     	m.OfflineQoS0,
		MaxRxPacketSize: 	m.MaxPacketSize,
		MaxRxTopicAlias: 	m.TopicAliasMaximum,
		MaxTxTopicAlias: 	0,
	}
}

func (m *Manager) configureSession(config *StartConfig, ses *session, id string, idGenerated bool) (*systree.ClientConnectStatus, error) {
	sub, sessionPresent := m.getSubscriber(id, config.Req.IsClean(), config.Req.Version())

	sConfig := &sessionReConfig{
		subscriber:       sub,
		will:             m.getWill(config.Req),
		killOnDisconnect: false,
	}

	cConfig := m.newConnectionPreConfig(config)

	if config.Req.Version() >= packet.ProtocolV50 {
		if err := readSessionProperties(config.Req, sConfig, cConfig); err != nil {
			return nil, err
		}

		ids := ""
		if idGenerated {
			ids = id
		}

		m.writeSessionProperties(config.Resp, ids)
		if err := config.Resp.PropertySet(packet.PropertyServerKeepAlive, m.KeepAlive); err != nil {
			return nil, err
		}
	}

	// MQTT v5 has different meaning of clean comparing to MQTT v3
	//  - v3: if session is clean it lasts when Network connection os close
	//  - v5: clean means clean start and server must wipe any previously created session with same id
	//        but keep this one if Network Connection is closed
	if (config.Req.Version() <= packet.ProtocolV311 && config.Req.IsClean()) ||
		(sConfig.expireIn != nil && *sConfig.expireIn == 0) {
		sConfig.killOnDisconnect = true
	}

	ses.reconfigure(sConfig, false)

	var status *systree.ClientConnectStatus

	if err := ses.allocConnection(cConfig); err == nil {
		if config.Req.IsClean() {
			m.persistence.Delete([]byte(id)) // nolint: errcheck
		}

		status = &systree.ClientConnectStatus{
			Username:          cConfig.Username,
			Timestamp:         time.Now().Format(time.RFC3339),
			ReceiveMaximum:    uint32(cConfig.SendQuota),
			MaximumPacketSize: cConfig.MaxTxPacketSize,
			GeneratedID:       idGenerated,
			SessionPresent:    sessionPresent,
			Address:           config.Conn.RemoteAddr().String(),
			KeepAlive:         config.Req.KeepAlive(),
			Protocol:          config.Req.Version(),
			ConnAckCode:       config.Resp.ReturnCode(),
			CleanSession:      config.Req.IsClean(),
			KillOnDisconnect:  sConfig.killOnDisconnect,
		}
	}

	config.Resp.SetSessionPresent(sessionPresent)

	return status, nil
}

func boolToByte(v bool) byte {
	if v {
		return 1
	}

	return 0
}

func readSessionProperties(req *packet.Connect, sc *sessionReConfig, cc *connection.PreConfig) (err error) {
	// [MQTT-3.1.2.11.2]
	if prop := req.PropertyGet(packet.PropertySessionExpiryInterval); prop != nil {
		if val, e := prop.AsInt(); e == nil {
			sc.expireIn = &val
		}
	}

	// [MQTT-3.1.2.11.3]
	if prop := req.PropertyGet(packet.PropertyWillDelayInterval); prop != nil {
		if val, e := prop.AsInt(); e == nil {
			sc.willDelay = val
		}
	}

	// [MQTT-3.1.2.11.4]
	if prop := req.PropertyGet(packet.PropertyReceiveMaximum); prop != nil {
		if val, e := prop.AsShort(); e == nil {
			cc.SendQuota = int32(val)
		}
	}

	// [MQTT-3.1.2.11.5]
	if prop := req.PropertyGet(packet.PropertyMaximumPacketSize); prop != nil {
		if val, e := prop.AsInt(); e == nil {
			cc.MaxTxPacketSize = val
		}
	}

	// [MQTT-3.1.2.11.6]
	if prop := req.PropertyGet(packet.PropertyTopicAliasMaximum); prop != nil {
		if val, e := prop.AsShort(); e == nil {
			cc.MaxTxTopicAlias = val
		}
	}

	// [MQTT-3.1.2.11.10]
	if prop := req.PropertyGet(packet.PropertyAuthMethod); prop != nil {
		if val, e := prop.AsString(); e == nil {
			cc.AuthMethod = val
		}
	}

	// [MQTT-3.1.2.11.11]
	if prop := req.PropertyGet(packet.PropertyAuthData); prop != nil {
		if len(cc.AuthMethod) == 0 {
			err = packet.CodeProtocolError
			return
		}
		if val, e := prop.AsBinary(); e == nil {
			cc.AuthData = val
		}
	}

	return
}

func (m *Manager) writeSessionProperties(resp *packet.ConnAck, id string) {
	// [MQTT-3.2.2.3.2] if server receive max less than 65536 than let client to know about
	if m.ReceiveMax < types.DefaultReceiveMax {
		resp.PropertySet(packet.PropertyReceiveMaximum, m.ReceiveMax) // nolint: errcheck
	}
	// [MQTT-3.2.2.3.3] if supported server's QoS less than 2 notify client
	if m.MaximumQoS < packet.QoS2 {
		resp.PropertySet(packet.PropertyMaximumQoS, byte(m.MaximumQoS)) // nolint: errcheck
	}
	// [MQTT-3.2.2.3.4] tell client whether retained messages supported
	resp.PropertySet(packet.PropertyRetainAvailable, boolToByte(m.AvailableRetain)) // nolint: errcheck
	// [MQTT-3.2.2.3.5] if server max packet size less than 268435455 than let client to know about
	if m.MaxPacketSize < types.DefaultMaxPacketSize {
		resp.PropertySet(packet.PropertyMaximumPacketSize, m.MaxPacketSize) // nolint: errcheck
	}
	// [MQTT-3.2.2.3.6]
	if len(id) > 0 {
		resp.PropertySet(packet.PropertyAssignedClientIdentifier, id) // nolint: errcheck
	}
	// [MQTT-3.2.2.3.7]
	if m.TopicAliasMaximum > 0 {
		resp.PropertySet(packet.PropertyTopicAliasMaximum, m.TopicAliasMaximum) // nolint: errcheck
	}
	// [MQTT-3.2.2.3.10] tell client whether server supports wildcard subscriptions or not
	resp.PropertySet(packet.PropertyWildcardSubscriptionAvailable, boolToByte(m.AvailableWildcardSubscription)) // nolint: errcheck
	// [MQTT-3.2.2.3.11] tell client whether server supports subscription identifiers or not
	resp.PropertySet(packet.PropertySubscriptionIdentifierAvailable, boolToByte(m.AvailableSubscriptionID)) // nolint: errcheck
	// [MQTT-3.2.2.3.12] tell client whether server supports shared subscriptions or not
	resp.PropertySet(packet.PropertySharedSubscriptionAvailable, boolToByte(m.AvailableSharedSubscription)) // nolint: errcheck
}

func (m *Manager) getSubscriber(id string, clean bool, v packet.ProtocolVersion) (subscriber.ConnectionProvider, bool) {
	var sub subscriber.ConnectionProvider
	var present bool

	if clean {
		if sb, ok := m.subscribers.Load(id); ok {
			sub = sb.(subscriber.ConnectionProvider)
			sub.Offline(true)
			m.subscribers.Delete(id)
		}
		if err := m.persistence.Delete([]byte(id)); err != nil && err != persistence.ErrNotFound {
			m.log.Error("Couldn't wipe session", zap.String("ClientID", id), zap.Error(err))
		}
	}

	if sb, ok := m.subscribers.Load(id); !ok {
		sub = subscriber.New(&subscriber.Config{
			ID:               id,
			Topics:           m.TopicsMgr,
			OnOfflinePublish: m.onPublish,
			OfflineQoS0:      m.OfflineQoS0,
			Version:          v,
		})

		present = m.persistence.Exists([]byte(id))
		m.subscribers.Store(id, sub)
		m.log.Debug("Subscriber created", zap.String("ClientID", id))
	} else {
		m.log.Debug("Subscriber obtained", zap.String("ClientID", id))
		sub = sb.(subscriber.ConnectionProvider)
		present = true
	}

	return sub, present
}

func (m *Manager) genClientID() string {
	b := make([]byte, 15)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}

	return base64.URLEncoding.EncodeToString(b)
}

func (m *Manager) onDisconnect(id string, reason packet.ReasonCode, retain bool) {
	m.log.Debug("Disconnected", zap.String("ClientID", id))
	m.Systree.Clients().Disconnected(id, reason, retain)
}

func (m *Manager) onSubscriberShutdown(sub subscriber.ConnectionProvider) {
	m.log.Debug("Shutdown subscriber", zap.String("ClientID", sub.ID()))
	sub.Offline(true)
	m.subscribers.Delete(sub.ID())
}

func (m *Manager) onSessionClose(id string, reason exitReason) {
	if reason == exitReasonClean || reason == exitReasonExpired {
		if err := m.persistence.Delete([]byte(id)); err != nil && err != persistence.ErrNotFound {
			m.log.Error("Couldn't wipe session", zap.String("ClientID", id), zap.Error(err))
		}

		rs := "clean"
		if reason == exitReasonExpired {
			rs = "expired"
		}

		state := &systree.SessionDeletedStatus{
			Timestamp: time.Now().Format(time.RFC3339),
			Reason:    rs,
		}

		m.Systree.Sessions().Removed(id, state)
	} else if s, ok := m.sessions.Load(id); ok{
		sub := s.(*sessionWrap).s.subscriber
		if sub != nil && sub.HasSubscriptions(){
			m.log.Debug("Persisting session state...", zap.String("ClientId", id), zap.Any("state", s.(*sessionWrap).s.getRuntimeState()))
			err := m.persistence.StateStore([]byte(id), s.(*sessionWrap).s.getRuntimeState())
			if err != nil {
				m.log.Warn("Session state persisting failed on close.", zap.String("ClientId", id), zap.Error(err))
			}
		} else {
			// if the client has no subscriptions, maybe no persistence are needed.
			m.log.Debug("Client has no subscriptions, deleting persisted session on close...", zap.String("ClientId", id))
			err := m.persistence.Delete([]byte(id))
			if err != nil {
				m.log.Warn("Failed to delete persisted session on close.", zap.String("ClientId", id), zap.Error(err))
			}
		}
	}

	m.log.Debug("Session close", zap.String("ClientID", id))

	m.sessions.Delete(id)
	m.sessionsCount.Done()
}

func (m *Manager) loadSessions() error {
	m.log.Info("Loading persisted sessions...")
	subs := map[string]*subscriberConfig{}

	delayedWills := []*packet.Publish{}

	err := m.persistence.LoadForEach(func(id []byte, state *persistence.SessionState) error {
		sID := string(id)
		if len(state.Errors) != 0 {
			m.log.Error("Session load", zap.String("ClientID", sID), zap.Errors("errors", state.Errors))
			// in separate routine for avoiding persistence dead lock.
			go func(){
			if err := m.persistence.SubscriptionsDelete(id); err != nil && err != persistence.ErrNotFound{
				m.log.Error("Persisted subscriber delete", zap.String("session_id", sID), zap.Error(err))
			} else {
				m.log.Debug("Persisted subscriber deleted", zap.String("session_id", sID))
			}}()
			return nil
		}
		if state.Expire != nil {
			since, err := time.Parse(time.RFC3339, state.Expire.Since)
			if err != nil {
				m.log.Error("Parse expiration value", zap.String("ClientID", sID), zap.Error(err))
				// in separate routine for avoiding persistence dead lock.
				go func() {
					if err := m.persistence.SubscriptionsDelete(id); err != nil && err != persistence.ErrNotFound {
						m.log.Error("Persisted subscriber delete", zap.Error(err))
					}
				}()
				return nil
			}

			var will *packet.Publish
			var willIn uint32
			var expireIn uint32

			// if persisted state has delayed will lets check if it has not elapsed its time
			if len(state.Expire.WillIn) > 0 && len(state.Expire.WillData) > 0 {
				pkt, _, _ := packet.Decode(packet.ProtocolVersion(state.Version), state.Expire.WillData)
				will, _ = pkt.(*packet.Publish)

				if val, err := strconv.Atoi(state.Expire.WillIn); err == nil {
					willIn = uint32(val)
					willAt := since.Add(time.Duration(willIn) * time.Second)

					if time.Now().Before(willAt) {
						// will delay elapsed. notify that
						delayedWills = append(delayedWills, will)
						will = nil
					}
				} else {
					m.log.Error("Decode will at", zap.String("ClientID", sID), zap.Error(err))
				}
			}

			if len(state.Expire.ExpireIn) > 0 {
				if val, err := strconv.Atoi(state.Expire.ExpireIn); err == nil {
					expireIn = uint32(val)
					expireAt := since.Add(time.Duration(expireIn) * time.Second)

					if time.Now().Before(expireAt) {
						// persisted session has expired, wipe it
						go func() {
							if err := m.persistence.Delete(id); err != nil && err != persistence.ErrNotFound {
								m.log.Error("Persisted session delete", zap.Error(err))
							}
						}()
						return nil
					}
				} else {
					m.log.Error("Decode expire at", zap.String("ClientID", sID), zap.Error(err))
				}
			}

			// persisted session has either delayed will or expiry
			// create it and run timer
			if will != nil || expireIn > 0 {
				createdAt, _ := time.Parse(time.RFC3339, state.Timestamp)
				ses := m.allocSession(sID, createdAt)
				var exp *uint32
				if expireIn > 0 {
					exp = &expireIn
				}

				setup := &sessionReConfig{
					subscriber:       nil,
					expireIn:         exp,
					will:             will,
					willDelay:        willIn,
					killOnDisconnect: false,
				}

				ses.s.reconfigure(setup, true)
				m.sessions.Store(id, ses)
				m.sessionsCount.Add(1)
				ses.release()
			}
		}

		if len(state.Subscriptions) > 0 {
			if sCfg, err := m.loadSubscriber(state.Subscriptions); err == nil {
				subs[sID] = sCfg
				m.log.Debug("Persisted subscriptions loaded for client:", zap.String("ClientID", sID), zap.Any("ver", sCfg.version), zap.Any("sub", sCfg.topics))
			} else {
				m.log.Error("Decode subscriber", zap.String("ClientID", sID), zap.Error(err))
			}
		}

		status := &systree.SessionCreatedStatus{
			Clean:     false,
			Timestamp: state.Timestamp,
		}

		m.Systree.Sessions().Created(sID, status)
		return nil
	})

	for id, t := range subs {
		sub := subscriber.New(
			&subscriber.Config{
				ID:               id,
				Topics:           m.TopicsMgr,
				OnOfflinePublish: m.onPublish,
				OfflineQoS0:      m.OfflineQoS0,
				Version:          t.version,
			})

		for topic, ops := range t.topics {
			if _, _, err = sub.Subscribe(topic, ops); err != nil {
				m.log.Error("Couldn't subscribe", zap.Error(err))
			}
		}
		m.subscribers.Store(id, sub)
	}

	// publish delayed wills if any
	for _, will := range delayedWills {
		if err = m.TopicsMgr.Publish(will); err != nil {
			m.log.Error("Publish delayed will", zap.Error(err))
		}
	}

	return err
}

func (m *Manager) loadSubscriber(from []byte) (*subscriberConfig, error) {
	subscriptions := subscriber.Subscriptions{}
	offset := 0
	version := packet.ProtocolVersion(from[offset])
	offset++
	remaining := len(from) - 1
	for offset < remaining {
		t, total, e := packet.ReadLPBytes(from[offset:])
		if e != nil {
			return nil, e
		}

		offset += total

		params := &topicsTypes.SubscriptionParams{}

		params.Ops = packet.SubscriptionOptions(from[offset])
		offset++

		params.ID = binary.BigEndian.Uint32(from[offset:])
		offset += 4
		subscriptions[string(t)] = params
	}

	return &subscriberConfig{
		version: version,
		topics:  subscriptions,
	}, nil
}

func (m *Manager) persistSubscriptions(id string, s subscriber.ConnectionProvider)(err error) {
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
		buf[offset] = byte(params.Ops)
		offset++
		binary.BigEndian.PutUint32(buf[offset:], params.ID)
		offset += 4
	}

	m.log.Debug("Persisting client subscriptions...", zap.String("ClientID", id), zap.Any("Subscrptions", topics))
	if err = m.persistence.SubscriptionsStore([]byte(id), buf); err != nil {
		m.log.Error("Couldn't persist subscriptions", zap.String("ClientID", id), zap.Error(err))
	}
	return err
}

func (m *Manager) storeSubscribers() error {
	// 4. shutdown and persist subscriptions from non-clean session
	//for id, s := range m.subscribers {
	m.subscribers.Range(func(k, v interface{}) bool {
		id := k.(string)
		s := v.(subscriber.ConnectionProvider)
		s.Offline(false)
		m.persistSubscriptions(id, s)
		return true
	})

	return nil
}

func (m *Manager) onPublish(id string, p *packet.Publish) {
	pkt := persistence.PersistedPacket{UnAck: false}

	if p.Expired(false) {
		return
	}

	if tm := p.GetExpiry(); !tm.IsZero() {
		pkt.ExpireAt = tm.Format(time.RFC3339)
	}

	p.SetPacketID(0)

	var err error
	pkt.Data, err = packet.Encode(p)
	if err != nil {
		m.log.Error("Couldn't encode packet", zap.String("ClientID", id), zap.Error(err))
		return
	}
	m.log.Debug("Persisting message packet:", zap.String("ClientID", id), zap.Int64("MessageUniqueID", p.GetCreateTimestamp()))
	if err = m.persistence.PacketStore([]byte(id), pkt); err != nil {
		m.log.Error("Couldn't persist message", zap.String("ClientID", id), zap.Error(err))
	}
}
