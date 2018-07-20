package clients

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/plugin/auth"
	"github.com/VolantMQ/vlapi/plugin/persistence"
	"github.com/VolantMQ/vlapi/subscriber"
	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/connection"
	"github.com/VolantMQ/volantmq/subscriber"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/topics/types"
	"github.com/VolantMQ/volantmq/transport"
	"github.com/VolantMQ/volantmq/types"
	"github.com/gosuri/uiprogress"
	"go.uber.org/zap"
)

// load sessions owning subscriptions
type subscriberConfig struct {
	version mqttp.ProtocolVersion
	topics  vlsubscriber.Subscriptions
}

// Config manager configuration
type Config struct {
	configuration.MqttConfig
	TopicsMgr        topicsTypes.Provider
	Persist          persistence.IFace
	Systree          systree.Provider
	OnReplaceAttempt func(string, bool)
	NodeName         string
}

type preloadConfig struct {
	exp *expiryConfig
	sub *subscriberConfig
}

// Manager clients manager
type Manager struct {
	persistence     persistence.Sessions
	log             *zap.SugaredLogger
	quit            chan struct{}
	sessionsCount   sync.WaitGroup
	sessions        sync.Map
	plSubscribers   map[string]vlsubscriber.IFace
	allowedVersions map[mqttp.ProtocolVersion]bool
	Config
}

// StartConfig used to reconfigure session after connection is created
type StartConfig struct {
	Req  *mqttp.Connect
	Resp *mqttp.ConnAck
	Conn net.Conn
	Auth vlauth.Permissions
}

type containerInfo struct {
	ses     *session
	sub     *subscriber.Type
	present bool
}

type loadContext struct {
	bar            *uiprogress.Bar
	preloadConfigs map[string]*preloadConfig
	delayedWills   []mqttp.IFace
}

// NewManager create new clients manager
func NewManager(c *Config) (*Manager, error) {
	var err error

	var m *Manager

	defer func() {
		if err != nil {

		}
	}()

	m = &Manager{
		Config: *c,
		quit:   make(chan struct{}),
		log:    configuration.GetLogger().Named("sessions"),
		allowedVersions: map[mqttp.ProtocolVersion]bool{
			mqttp.ProtocolV31:  false,
			mqttp.ProtocolV311: false,
			mqttp.ProtocolV50:  false,
		},
		plSubscribers: make(map[string]vlsubscriber.IFace),
	}

	m.persistence, _ = c.Persist.Sessions()

	for _, v := range m.Version {
		switch v {
		case "v3.1":
			m.allowedVersions[mqttp.ProtocolV31] = true
		case "v3.1.1":
			m.allowedVersions[mqttp.ProtocolV311] = true
		case "v5.0":
			m.allowedVersions[mqttp.ProtocolV50] = true
		default:
			return nil, errors.New("unknown MQTT protocol: " + v)
		}
	}

	pCount := m.persistence.Count()
	if pCount > 0 {
		m.log.Info("Loading sessions. Might take a while")
		m.log.Sync()

		uiprogress.Start()
		bar := uiprogress.AddBar(int(pCount)).AppendCompleted().PrependElapsed()

		bar.PrependFunc(func(b *uiprogress.Bar) string {
			return fmt.Sprintf("Session load (%d/%d)", b.Current(), int(pCount))
		})

		context := &loadContext{
			bar:            bar,
			preloadConfigs: make(map[string]*preloadConfig),
		}

		// load sessions for fill systree
		// those sessions having either will delay or expire are created with and timer started
		err = m.persistence.LoadForEach(m, context)

		uiprogress.Stop()

		if err != nil {
			return nil, err
		}

		m.configurePersistedSubscribers(context)
		m.configurePersistedExpiry(context)
		m.processDelayedWills(context)

		for id, st := range context.preloadConfigs {
			if st.sub != nil {
				m.persistence.SubscriptionsDelete([]byte(id))
			}
			if st.exp != nil {
				m.persistence.ExpiryDelete([]byte(id))
			}
		}

		m.log.Info("Sessions loaded")
	} else {
		m.log.Info("No persisted sessions")
	}

	return m, nil
}

// Stop session manager. Stops any existing connections
func (m *Manager) Stop() error {
	select {
	case <-m.quit:
		return errors.New("already stopped")
	default:
		close(m.quit)
	}

	// stop running sessions
	m.sessions.Range(func(k, v interface{}) bool {
		wrap := v.(*container)
		wrap.rmLock.Lock()
		ses := wrap.ses
		wrap.rmLock.Unlock()

		if ses != nil {
			ses.stop(mqttp.CodeServerShuttingDown)
		} else {
			m.sessionsCount.Done()
		}

		exp := wrap.expiry.Load()
		if exp != nil {
			e := exp.(*expiry)
			e.cancel()

			m.persistence.ExpiryStore([]byte(k.(string)), e.persistedState())
		}

		return true
	})

	m.sessionsCount.Wait()

	return nil
}

// Shutdown gracefully by stopping all active sessions and persist states
func (m *Manager) Shutdown() error {
	// shutdown subscribers
	m.sessions.Range(func(k, v interface{}) bool {
		wrap := v.(*container)
		if wrap.sub != nil {
			if err := m.persistSubscriber(wrap.sub); err != nil {
				m.log.Error("persist subscriber", zap.Error(err))
			}
		}

		m.sessions.Delete(k)

		return true
	})

	return nil
}

// GetSubscriber ...
func (m *Manager) GetSubscriber(id string) (vlsubscriber.IFace, error) {
	sub, ok := m.plSubscribers[id]

	if !ok {
		sub = subscriber.New(subscriber.Config{
			ID: id,
			//OfflinePublish: m.pluginPublish,
		})
		m.plSubscribers[id] = sub
	}

	return sub, nil
}

// LoadSession load persisted session. Invoked by persistence provider
func (m *Manager) LoadSession(context interface{}, id []byte, state *persistence.SessionState) error {
	sID := string(id)
	ctx := context.(*loadContext)

	defer ctx.bar.Incr()

	if len(state.Errors) != 0 {
		m.log.Error("Session load", zap.String("ClientID", sID), zap.Errors("errors", state.Errors))
		// if err := m.persistence.SubscriptionsDelete(id); err != nil && err != persistence.ErrNotFound {
		//	m.log.Error("Persisted subscriber delete", zap.Error(err))
		// }

		return nil
	}

	var err error

	status := &systree.SessionCreatedStatus{
		Clean:     false,
		Timestamp: state.Timestamp,
	}

	if err = m.decodeSessionExpiry(ctx, sID, state); err != nil {
		m.log.Error("Decode session expiry", zap.String("ClientID", sID), zap.Error(err))
	}

	if err = m.decodeSubscriber(ctx, sID, state.Subscriptions); err != nil {
		m.log.Error("Decode subscriber", zap.String("ClientID", sID), zap.Error(err))
		if err = m.persistence.SubscriptionsDelete(id); err != nil && err != persistence.ErrNotFound {
			m.log.Error("Persisted subscriber delete", zap.Error(err))
		}
	}

	if cfg, ok := ctx.preloadConfigs[sID]; ok && cfg.exp != nil {
		status.WillDelay = strconv.FormatUint(uint64(cfg.exp.willIn), 10)
		if cfg.exp.expireIn != nil {
			status.ExpiryInterval = strconv.FormatUint(uint64(*cfg.exp.expireIn), 10)
		}
	}

	m.Systree.Sessions().Created(sID, status)
	return nil
}

// OnConnection implements transport.Handler interface and handles incoming connection
func (m *Manager) OnConnection(conn transport.Conn, authMngr *auth.Manager) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			err = errors.New("panic")
		}
	}()
	cn := connection.New(
		connection.OnAuth(m.onAuth),
		connection.NetConn(conn),
		connection.TxQuota(types.DefaultReceiveMax),
		connection.RxQuota(int32(m.Options.ReceiveMax)),
		connection.Metric(m.Systree.Metric().Packets()),
		connection.RetainAvailable(m.Options.RetainAvailable),
		connection.OfflineQoS0(m.Options.OfflineQoS0),
		connection.MaxTxPacketSize(types.DefaultMaxPacketSize),
		connection.MaxRxPacketSize(m.Options.MaxPacketSize),
		connection.MaxRxTopicAlias(m.Options.MaxTopicAlias),
		connection.MaxTxTopicAlias(0),
		connection.KeepAlive(m.Options.ConnectTimeout),
		connection.Persistence(m.persistence),
	)

	var connParams *connection.ConnectParams
	var ack *mqttp.ConnAck
	if ch, e := cn.Accept(); e == nil {
		for dl := range ch {
			var resp mqttp.IFace
			switch obj := dl.(type) {
			case *connection.ConnectParams:
				connParams = obj
				resp, e = m.processConnect(cn, connParams, authMngr)
			case connection.AuthParams:
				resp, e = m.processAuth(connParams, obj)
			case error:
				e = obj
			default:
				e = errors.New("unknown")
			}

			if e != nil || resp == nil {
				cn.Stop(e)
				cn = nil
				return nil
			} else {
				if resp.Type() == mqttp.AUTH {
					cn.Send(resp)
				} else {
					ack = resp.(*mqttp.ConnAck)
					break
				}
			}
		}
	}

	m.newSession(cn, connParams, ack, authMngr)

	return nil
}

func (m *Manager) processConnect(cn connection.Initial, params *connection.ConnectParams, authMngr *auth.Manager) (mqttp.IFace, error) {
	var resp mqttp.IFace

	if allowed, ok := m.allowedVersions[params.Version]; !ok || !allowed {
		reason := mqttp.CodeRefusedUnacceptableProtocolVersion
		if params.Version == mqttp.ProtocolV50 {
			reason = mqttp.CodeUnsupportedProtocol
		}

		return nil, reason
	}

	if len(params.AuthMethod) > 0 {
		// TODO(troian): verify method is allowed
	} else {
		var reason mqttp.ReasonCode

		if status := authMngr.Password(params.ID, string(params.Username), string(params.Password)); status == vlauth.StatusAllow {
			reason = mqttp.CodeSuccess
		} else {
			reason = mqttp.CodeRefusedBadUsernameOrPassword
			if params.Version == mqttp.ProtocolV50 {
				reason = mqttp.CodeBadUserOrPassword
			}
		}

		pkt := mqttp.NewConnAck(params.Version)
		pkt.SetReturnCode(reason)
		resp = pkt
	}

	return resp, nil
}

func (m *Manager) processAuth(params *connection.ConnectParams, auth connection.AuthParams) (mqttp.IFace, error) {
	var resp mqttp.IFace

	return resp, nil
}

// newSession create new session with provided established connection
func (m *Manager) newSession(cn connection.Initial, params *connection.ConnectParams, ack *mqttp.ConnAck, authMngr *auth.Manager) {
	var ses *session
	var err error

	defer func() {
		keepAlive := int(params.KeepAlive)
		if m.KeepAlive.Force || params.KeepAlive > 0 {
			if m.KeepAlive.Force {
				keepAlive = m.KeepAlive.Period
			}
		}

		if cn.Acknowledge(ack, connection.KeepAlive(keepAlive)) {
			ses.start()

			// TODO(troian): add remote address
			status := &systree.ClientConnectStatus{
				Username:          string(params.Username),
				Timestamp:         time.Now().Format(time.RFC3339),
				ReceiveMaximum:    uint32(params.SendQuota),
				MaximumPacketSize: params.MaxTxPacketSize,
				GeneratedID:       params.IDGen,
				SessionPresent:    ack.SessionPresent(),
				KeepAlive:         uint16(keepAlive),
				ConnAckCode:       ack.ReturnCode(),
				Protocol:          params.Version,
				CleanSession:      params.CleanStart,
				Durable:           params.Durable,
			}

			m.Systree.Clients().Connected(params.ID, status)
		}
	}()

	// if response has return code differs from CodeSuccess return from this point
	// and send connack in deferred statement
	if ack.ReturnCode() != mqttp.CodeSuccess {
		return
	}

	if params.Version >= mqttp.ProtocolV50 {
		ids := ""
		if params.IDGen {
			ids = params.ID
		}

		if err = m.writeSessionProperties(ack, ids); err != nil {
			reason := mqttp.CodeUnspecifiedError
			if params.Version <= mqttp.ProtocolV50 {
				reason = mqttp.CodeRefusedServerUnavailable
			}
			ack.SetReturnCode(reason)
			return
		}
	}

	var info *containerInfo
	if info, err = m.loadContainer(cn.Session(), params, authMngr); err == nil {
		ses = info.ses
		config := sessionConfig{
			sessionEvents: m,
			expireIn:      params.ExpireIn,
			will:          params.Will,
			durable:       params.Durable,
			version:       params.Version,
			subscriber:    info.sub,
		}

		ses.configure(config)

		ack.SetSessionPresent(info.present)
	} else {
		var reason mqttp.ReasonCode
		if r, ok := err.(mqttp.ReasonCode); ok {
			reason = r
		} else {
			reason = mqttp.CodeUnspecifiedError
			if params.Version <= mqttp.ProtocolV50 {
				reason = mqttp.CodeRefusedServerUnavailable
			}
		}

		ack.SetReturnCode(reason)
	}
}

func (m *Manager) onAuth(id string, params *connection.AuthParams) (mqttp.IFace, error) {
	return nil, nil
}

func (m *Manager) checkServerStatus(v mqttp.ProtocolVersion, resp *mqttp.ConnAck) {
	// check first if server is not about to shutdown
	// if so just give reject and exit
	select {
	case <-m.quit:
		var reason mqttp.ReasonCode
		switch v {
		case mqttp.ProtocolV50:
			reason = mqttp.CodeServerShuttingDown
			// TODO: if cluster route client to another node
		default:
			reason = mqttp.CodeRefusedServerUnavailable
		}
		if err := resp.SetReturnCode(reason); err != nil {
			m.log.Error("check server status set return code", zap.Error(err))
		}
	default:
	}
}

// allocContainer
func (m *Manager) allocContainer(id string, username string, authMngr *auth.Manager, createdAt time.Time, cn connection.Session) *container {
	ses := newSession(sessionPreConfig{
		id:          id,
		createdAt:   createdAt,
		conn:        cn,
		messenger:   m.TopicsMgr,
		persistence: m.persistence,
		permissions: authMngr,
		username:    username,
	})

	cont := &container{
		removable: true,
		removed:   false,
	}

	ses.idLock = &cont.lock
	cont.ses = ses
	cont.acquire()

	return cont
}

func (m *Manager) loadContainer(cn connection.Session, params *connection.ConnectParams, authMngr *auth.Manager) (cont *containerInfo, err error) {
	newContainer := m.allocContainer(params.ID, string(params.Username), authMngr, time.Now(), cn)

	// search for existing container with given id
	if curr, present := m.sessions.LoadOrStore(params.ID, newContainer); present {
		// container with given id already exists with either active connection or expiry/willDelay set

		// release lock of newly allocated container as lock from old one will be used
		newContainer.release()

		currContainer := curr.(*container)

		// lock id to prevent other incoming connections with same ID making any changes until we done
		currContainer.acquire()
		currContainer.setRemovable(false)

		if current := currContainer.session(); current != nil {
			// container has session with active connection

			m.OnReplaceAttempt(params.ID, m.Options.SessionDups)
			if !m.Options.SessionDups {
				// we do not make any changes to current network connection
				// response to new one with error and release both new & old sessions
				err = mqttp.CodeRefusedIdentifierRejected
				if params.Version >= mqttp.ProtocolV50 {
					err = mqttp.CodeInvalidClientID
				}

				currContainer.setRemovable(true)

				currContainer.release()
				newContainer = nil
				return
			}

			// session will be replaced with new one
			// stop current active connection
			current.stop(mqttp.CodeSessionTakenOver)
		}

		// MQTT5.0 cancel expiry if set
		if val := currContainer.expiry.Load(); val != nil {
			exp := val.(*expiry)
			exp.cancel()
			currContainer.expiry = atomic.Value{}
		}

		currContainer.rmLock.Lock()
		removed := currContainer.removed
		currContainer.rmLock.Unlock()

		if removed {
			// if current container marked as removed check if concurrent connection has created new entry with same id
			// and reject current if so
			if _, present = m.sessions.LoadOrStore(params.ID, newContainer); present {
				err = mqttp.CodeRefusedIdentifierRejected
				if params.Version >= mqttp.ProtocolV50 {
					err = mqttp.CodeInvalidClientID
				}
				return
			} else {
				m.sessionsCount.Add(1)
			}
		} else {
			newContainer = currContainer.swap(newContainer)
			newContainer.removed = false
			newContainer.setRemovable(true)
		}
	} else {
		m.sessionsCount.Add(1)
	}

	sub := newContainer.subscriber(
		params.CleanStart,
		subscriber.Config{
			ID:             params.ID,
			OfflinePublish: m.sessionPersistPublish,
			Topics:         m.TopicsMgr,
			Version:        params.Version,
		})

	if params.CleanStart {
		if err = m.persistence.Delete([]byte(params.ID)); err != nil && err != persistence.ErrNotFound {
			m.log.Error("Couldn't wipe session", zap.String("ClientID", params.ID), zap.Error(err))
		} else {
			err = nil
		}
	}

	persisted := m.persistence.Exists([]byte(params.ID))

	if !persisted {
		err = m.persistence.Create([]byte(params.ID),
			&persistence.SessionBase{
				Timestamp: time.Now().Format(time.RFC3339),
				Version:   byte(params.Version),
			})
		if err != nil {
			m.log.Error("Create persistence entry: ", err.Error())
		}
	}

	if err == nil {
		if !persisted {
			status := &systree.SessionCreatedStatus{
				Clean:     params.CleanStart,
				Durable:   params.Durable,
				Timestamp: time.Now().Format(time.RFC3339),
			}
			m.Systree.Sessions().Created(params.ID, status)
		}

		cont = &containerInfo{
			ses:     newContainer.ses,
			sub:     sub,
			present: persisted,
		}
	}

	return
}

func (m *Manager) writeSessionProperties(resp *mqttp.ConnAck, id string) error {
	boolToByte := func(v bool) byte {
		if v {
			return 1
		}

		return 0
	}

	// [MQTT-3.2.2.3.2] if server receive max less than 65535 than let client to know about
	if m.Options.ReceiveMax < types.DefaultReceiveMax {
		if err := resp.PropertySet(mqttp.PropertyReceiveMaximum, m.Options.ReceiveMax); err != nil {
			return err
		}
	}

	// [MQTT-3.2.2.3.3] if supported server's QoS less than 2 notify client
	if m.Options.MaxQoS < mqttp.QoS2 {
		if err := resp.PropertySet(mqttp.PropertyMaximumQoS, byte(m.Options.MaxQoS)); err != nil {
			return err
		}
	}
	// [MQTT-3.2.2.3.4] tell client whether retained messages supported
	if err := resp.PropertySet(mqttp.PropertyRetainAvailable, boolToByte(m.Options.RetainAvailable)); err != nil {
		return err
	}
	// [MQTT-3.2.2.3.5] if server max packet size less than 268435455 than let client to know about
	if m.Options.MaxPacketSize < types.DefaultMaxPacketSize {
		if err := resp.PropertySet(mqttp.PropertyMaximumPacketSize, m.Options.MaxPacketSize); err != nil {
			return err
		}
	}
	// [MQTT-3.2.2.3.6]
	if len(id) > 0 {
		if err := resp.PropertySet(mqttp.PropertyAssignedClientIdentifier, id); err != nil {
			return err
		}
	}
	// [MQTT-3.2.2.3.7]
	if m.Options.MaxTopicAlias > 0 {
		if err := resp.PropertySet(mqttp.PropertyTopicAliasMaximum, m.Options.MaxTopicAlias); err != nil {
			return err
		}
	}
	// [MQTT-3.2.2.3.10] tell client whether server supports wildcard subscriptions or not
	if err := resp.PropertySet(mqttp.PropertyWildcardSubscriptionAvailable, boolToByte(m.Options.SubsWildcard)); err != nil {
		return err
	}
	// [MQTT-3.2.2.3.11] tell client whether server supports subscription identifiers or not
	if err := resp.PropertySet(mqttp.PropertySubscriptionIdentifierAvailable, boolToByte(m.Options.SubsID)); err != nil {
		return err
	}
	// [MQTT-3.2.2.3.12] tell client whether server supports shared subscriptions or not
	if err := resp.PropertySet(mqttp.PropertySharedSubscriptionAvailable, boolToByte(m.Options.SubsShared)); err != nil {
		return err
	}

	if m.KeepAlive.Force {
		if err := resp.PropertySet(mqttp.PropertyServerKeepAlive, m.KeepAlive); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) connectionClosed(id string, reason mqttp.ReasonCode) {
	m.Systree.Clients().Disconnected(id, reason)
}

func (m *Manager) subscriberShutdown(id string, sub vlsubscriber.IFace) {
	sub.Offline(true)
	if val, ok := m.sessions.Load(id); ok {
		wrap := val.(*container)
		wrap.sub = nil
	} else {
		m.log.Error("subscriber shutdown. container not found", zap.String("ClientID", id))
	}
}

func (m *Manager) sessionOffline(id string, keep bool, expCfg *expiryConfig) {
	if obj, ok := m.sessions.Load(id); ok {
		if cont, kk := obj.(*container); kk {
			cont.rmLock.Lock()
			cont.ses = nil

			if keep {
				if expCfg != nil {
					expCfg.expiryEvent = m
					exp := newExpiry(*expCfg)
					cont.expiry.Store(exp)

					exp.start()
				}
			} else {
				if cont.removable {
					state := &systree.SessionDeletedStatus{
						Timestamp: time.Now().Format(time.RFC3339),
						Reason:    "",
					}

					m.Systree.Sessions().Removed(id, state)
					m.sessions.Delete(id)
					m.sessionsCount.Done()
					cont.removed = true
				}
			}
			cont.rmLock.Unlock()
		} else {
			m.log.Panic("is not a container")
		}
	} else {
		m.log.Error("Couldn't wipe session, object does not exist")
	}
}

func (m *Manager) sessionTimer(id string, expired bool) {
	rs := "shutdown"
	if expired {
		rs = "expired"

		m.persistence.Delete([]byte(id))

		m.sessions.Delete(id)
		m.sessionsCount.Done()
	}

	state := &systree.SessionDeletedStatus{
		Timestamp: time.Now().Format(time.RFC3339),
		Reason:    rs,
	}

	m.Systree.Sessions().Removed(id, state)
}

func (m *Manager) configurePersistedSubscribers(ctx *loadContext) {
	for id, t := range ctx.preloadConfigs {
		sub := subscriber.New(
			subscriber.Config{
				ID:             id,
				Topics:         m.TopicsMgr,
				OfflinePublish: m.sessionPersistPublish,
				Version:        t.sub.version,
			})

		for topic, ops := range t.sub.topics {
			if _, _, err := sub.Subscribe(topic, ops); err != nil {
				m.log.Error("Couldn't subscribe", zap.Error(err))
			}
		}

		cont := &container{
			removable: true,
			removed:   false,
			sub:       sub,
		}

		m.sessions.Store(id, cont)
		m.sessionsCount.Add(1)
	}
}

func (m *Manager) configurePersistedExpiry(ctx *loadContext) {
	for id, t := range ctx.preloadConfigs {
		cont := &container{
			removable: true,
			removed:   false,
		}

		exp := newExpiry(*t.exp)

		cont.expiry.Store(exp)
		if c, present := m.sessions.LoadOrStore(id, cont); present {
			cnt := c.(*container)
			cnt.expiry.Store(exp)
		}

		exp.start()
	}
}

func (m *Manager) processDelayedWills(ctx *loadContext) {
	for _, will := range ctx.delayedWills {
		if err := m.TopicsMgr.Publish(will); err != nil {
			m.log.Error("Publish delayed will", zap.Error(err))
		}
	}
}

// decodeSessionExpiry
func (m *Manager) decodeSessionExpiry(ctx *loadContext, id string, state *persistence.SessionState) error {
	if state.Expire == nil {
		return nil
	}

	var err error
	var since time.Time

	if len(state.Expire.Since) > 0 {
		since, err = time.Parse(time.RFC3339, state.Expire.Since)
		if err != nil {
			m.log.Error("parse expiration value", zap.String("clientId", id), zap.Error(err))
			if e := m.persistence.SubscriptionsDelete([]byte(id)); e != nil && e != persistence.ErrNotFound {
				m.log.Error("Persisted subscriber delete", zap.Error(e))
			}

			return err
		}
	}
	var will *mqttp.Publish
	var willIn uint32
	var expireIn uint32

	// if persisted state has delayed will lets check if it has not elapsed its time
	if len(state.Expire.Will) > 0 {
		pkt, _, _ := mqttp.Decode(mqttp.ProtocolVersion(mqttp.ProtocolV50), state.Expire.Will)
		will, _ = pkt.(*mqttp.Publish)

		if prop := pkt.PropertyGet(mqttp.PropertyWillDelayInterval); prop != nil {
			willIn, _ = prop.AsInt()
			willAt := since.Add(time.Duration(willIn) * time.Second)
			if time.Now().After(willAt) {
				// will delay elapsed. notify keep in list and publish when all persisted sessions loaded
				ctx.delayedWills = append(ctx.delayedWills, will)
				will = nil
				willIn = 0
			}
		}
	}

	if len(state.Expire.ExpireIn) > 0 {
		var val int
		if val, err = strconv.Atoi(state.Expire.ExpireIn); err == nil {
			expireIn = uint32(val)
			expireAt := since.Add(time.Duration(expireIn) * time.Second)

			if time.Now().After(expireAt) {
				// persisted session has expired, wipe it
				if err = m.persistence.Delete([]byte(id)); err != nil && err != persistence.ErrNotFound {
					m.log.Error("Delete expired session", zap.Error(err))
				}
				return nil
			}
		} else {
			m.log.Error("Decode expire at", zap.String("clientId", id), zap.Error(err))
		}
	}

	// persisted session has either delayed will or expiry
	// create it and run timer
	if will != nil || expireIn > 0 {
		var createdAt time.Time
		if createdAt, err = time.Parse(time.RFC3339, state.Timestamp); err != nil {
			m.log.Named("persistence").Error("Decode createdAt failed, using current timestamp",
				zap.String("clientId", id),
				zap.Error(err))
			createdAt = time.Now()
		}

		if _, ok := ctx.preloadConfigs[id]; !ok {
			ctx.preloadConfigs[id] = &preloadConfig{}
		}

		var expiringSince time.Time

		if expireIn > 0 {
			if expiringSince, err = time.Parse(time.RFC3339, state.Expire.Since); err != nil {
				m.log.Named("persistence").Error("Decode Expire.Since failed",
					zap.String("clientId", id),
					zap.Error(err))
			}
		}

		ctx.preloadConfigs[id].exp = &expiryConfig{
			expiryEvent:   m,
			messenger:     m.TopicsMgr,
			createdAt:     createdAt,
			expiringSince: expiringSince,
			will:          will,
			willIn:        willIn,
			expireIn:      &expireIn,
		}
	}

	return nil
}

// decodeSubscriber function invoke only during server startup. Used to decode persisted session
// which has active subscriptions
func (m *Manager) decodeSubscriber(ctx *loadContext, id string, from []byte) error {
	if len(from) == 0 {
		return nil
	}

	subscriptions := vlsubscriber.Subscriptions{}
	offset := 0
	version := mqttp.ProtocolVersion(from[offset])
	offset++
	remaining := len(from) - 1
	for offset != remaining {
		t, total, e := mqttp.ReadLPBytes(from[offset:])
		if e != nil {
			return e
		}

		offset += total

		params := &vlsubscriber.SubscriptionParams{}

		params.Ops = mqttp.SubscriptionOptions(from[offset])
		offset++

		params.ID = binary.BigEndian.Uint32(from[offset:])
		offset += 4
		subscriptions[string(t)] = params
	}

	if _, ok := ctx.preloadConfigs[id]; !ok {
		ctx.preloadConfigs[id] = &preloadConfig{}
	}

	ctx.preloadConfigs[id].sub = &subscriberConfig{
		version: version,
		topics:  subscriptions,
	}

	return nil
}

func (m *Manager) persistSubscriber(s *subscriber.Type) error {
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
	buf[offset] = byte(s.GetVersion())
	offset++

	for topic, params := range topics {
		total, _ := mqttp.WriteLPBytes(buf[offset:], []byte(topic))
		offset += total
		buf[offset] = byte(params.Ops)
		offset++
		binary.BigEndian.PutUint32(buf[offset:], params.ID)
		offset += 4
	}

	if err := m.persistence.SubscriptionsStore([]byte(s.ID), buf); err != nil {
		m.log.Error("Couldn't persist subscriptions", zap.String("ClientID", s.ID), zap.Error(err))
	}

	s.Offline(true)
	return nil
}

func (m *Manager) sessionPersistPublish(id string, p *mqttp.Publish) {
	pkt := &persistence.PersistedPacket{}

	var expired bool
	var expireAt time.Time

	if expireAt, _, expired = p.Expired(); expired {
		return
	}

	if !expireAt.IsZero() {
		pkt.ExpireAt = expireAt.Format(time.RFC3339)
	}

	p.SetPacketID(0)

	var err error
	pkt.Data, err = mqttp.Encode(p)
	if err != nil {
		m.log.Error("Couldn't encode packet", zap.String("ClientID", id), zap.Error(err))
		return
	}

	if p.QoS() == mqttp.QoS0 {
		err = m.persistence.PacketStoreQoS0([]byte(id), pkt)
	} else {
		m.persistence.PacketStoreQoS12([]byte(id), pkt)
	}

	if err != nil {
		m.log.Error("Couldn't persist message", zap.String("ClientID", id), zap.Error(err))
	}
}
