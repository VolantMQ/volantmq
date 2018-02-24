package clients

import (
	"container/list"
	"sync"
	"time"

	"github.com/VolantMQ/persistence"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/connection"
	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/subscriber"
	"github.com/VolantMQ/volantmq/topics/types"
	"github.com/VolantMQ/volantmq/types"
	"go.uber.org/zap"
)

type sessionEvents interface {
	sessionOffline(string, bool, *expiry)
	connectionClosed(string, packet.ReasonCode)
	subscriberShutdown(string, subscriber.SessionProvider)
}

type sessionPreConfig struct {
	id          string
	createdAt   time.Time
	messenger   types.TopicMessenger
	conn        connection.Session
	persistence persistence.Packets
}

type sessionConfig struct {
	sessionEvents
	subscriber subscriber.SessionProvider
	will       *packet.Publish
	expireIn   *uint32
	willDelay  uint32
	durable    bool
	version    packet.ProtocolVersion
}

type session struct {
	sessionPreConfig
	log     *zap.SugaredLogger
	idLock  *sync.Mutex
	lock    sync.Mutex
	stopReq types.Once
	sessionConfig
}

type temporaryPublish struct {
	gList *list.List
	qList *list.List
}

func newTmpPublish() *temporaryPublish {
	return &temporaryPublish{
		gList: list.New(),
		qList: list.New(),
	}
}

func (t *temporaryPublish) Publish(id string, p *packet.Publish) {
	if p.QoS() == packet.QoS0 {
		t.gList.PushBack(p)
	} else {
		t.qList.PushBack(p)
	}
}

func newSession(c sessionPreConfig) *session {
	s := &session{
		sessionPreConfig: c,
		log:              configuration.GetLogger().Named("session"),
	}

	return s
}

func (s *session) configure(c sessionConfig, clean bool) {
	s.sessionConfig = c

	s.conn.SetOptions(connection.AttachSession(s))

	if !clean {
		tmp := newTmpPublish()
		s.subscriber.Online(tmp)
		s.persistence.PacketsForEach([]byte(s.id), s.conn)
		s.subscriber.Online(s.conn)
		s.conn.LoadRemaining(tmp.gList, tmp.qList)
	} else {
		s.subscriber.Online(s.conn)
	}
}

func (s *session) start() {
	s.idLock.Unlock()
}

func (s *session) stop(reason packet.ReasonCode) {
	s.stopReq.Do(func() {
		s.conn.Stop(reason)
	})

	//s.wgDisconnected.Wait()

	//state := &persistence.SessionState{
	//	Timestamp: s.createdAt.Format(time.RFC3339),
	//}

	//if s.expireIn != nil || (s.willDelay > 0 && s.will != nil) {
	//	state.Expire = &persistence.SessionDelays{
	//		Since: s.expiringSince.Format(time.RFC3339),
	//	}
	//
	//	elapsed := uint32(time.Since(s.expiringSince) / time.Second)
	//
	//	if (s.willDelay > 0 && s.will != nil) && (s.willDelay-elapsed) > 0 {
	//		s.willDelay = s.willDelay - elapsed
	//		s.will.SetPacketID(0)
	//		if buf, err := packet.Encode(s.will); err != nil {
	//
	//		} else {
	//			state.Expire.WillIn = strconv.Itoa(int(s.willDelay))
	//			state.Expire.WillData = buf
	//		}
	//	}
	//
	//	if s.expireIn != nil && *s.expireIn > 0 && (*s.expireIn-elapsed) > 0 {
	//		*s.expireIn = *s.expireIn - elapsed
	//	}
	//}

	//return state
}

// SignalPublish process PUBLISH packet from client
func (s *session) SignalPublish(pkt *packet.Publish) error {
	pkt.SetPublishID(s.subscriber.Hash())

	// [MQTT-3.3.1.3]
	if pkt.Retain() {
		if err := s.messenger.Retain(pkt); err != nil {
			s.log.Error("Error retaining message", zap.String("ClientID", s.id), zap.Error(err))
		}

		// [MQTT-3.3.1-7]
		if pkt.QoS() == packet.QoS0 {
			retained := packet.NewPublish(s.version)
			retained.SetQoS(pkt.QoS())     // nolint: errcheck
			retained.SetTopic(pkt.Topic()) // nolint: errcheck
			//s.retained.list = append(s.retained.list, m)
		}
	}

	if err := s.messenger.Publish(pkt); err != nil {
		s.log.Error("Couldn't publish", zap.String("ClientID", s.id), zap.Error(err))
	}

	return nil
}

// SignalSubscribe process SUBSCRIBE packet from client
func (s *session) SignalSubscribe(pkt *packet.Subscribe) (packet.Provider, error) {
	m, _ := packet.New(s.version, packet.SUBACK)
	resp, _ := m.(*packet.SubAck)

	id, _ := pkt.ID()
	resp.SetPacketID(id)

	var retCodes []packet.ReasonCode
	var retainedPublishes []*packet.Publish

	pkt.RangeTopics(func(t string, ops packet.SubscriptionOptions) {
		reason := packet.CodeSuccess // nolint: ineffassign
		//authorized := true
		// TODO: check permissions here

		//if authorized {
		subsID := uint32(0)

		// V5.0 [MQTT-3.8.2.1.2]
		if prop := pkt.PropertyGet(packet.PropertySubscriptionIdentifier); prop != nil {
			if v, e := prop.AsInt(); e == nil {
				subsID = v
			}
		}

		subsParams := topicsTypes.SubscriptionParams{
			ID:  subsID,
			Ops: ops,
		}

		if grantedQoS, retained, err := s.subscriber.Subscribe(t, &subsParams); err != nil {
			// [MQTT-3.9.3]
			if s.version == packet.ProtocolV50 {
				reason = packet.CodeUnspecifiedError
			} else {
				reason = packet.QosFailure
			}
		} else {
			reason = packet.ReasonCode(grantedQoS)
			retainedPublishes = append(retainedPublishes, retained...)
		}

		retCodes = append(retCodes, reason)
	})

	if err := resp.AddReturnCodes(retCodes); err != nil {
		return nil, err
	}

	// Now put retained messages into publish queue
	for _, rp := range retainedPublishes {
		if pkt, err := rp.Clone(s.version); err == nil {
			pkt.SetRetain(true)
			s.conn.Publish(s.id, pkt)
		} else {
			s.log.Error("Couldn't clone PUBLISH message", zap.String("ClientID", s.id), zap.Error(err))
		}
	}

	return resp, nil
}

// SignalUnSubscribe process UNSUBSCRIBE packet from client
func (s *session) SignalUnSubscribe(pkt *packet.UnSubscribe) (packet.Provider, error) {
	var retCodes []packet.ReasonCode

	for _, t := range pkt.Topics() {
		// TODO: check permissions here
		authorized := true
		reason := packet.CodeSuccess

		if authorized {
			if err := s.subscriber.UnSubscribe(t); err != nil {
				s.log.Error("Couldn't unsubscribe from topic", zap.Error(err))
				reason = packet.CodeNoSubscriptionExisted
			}
		} else {
			reason = packet.CodeNotAuthorized
		}

		retCodes = append(retCodes, reason)
	}

	m, _ := packet.New(s.version, packet.UNSUBACK)
	resp, _ := m.(*packet.UnSubAck)

	id, _ := pkt.ID()
	resp.SetPacketID(id)
	resp.AddReturnCodes(retCodes) // nolint: errcheck

	return resp, nil
}

// SignalDisconnect process DISCONNECT packet from client
func (s *session) SignalDisconnect(pkt *packet.Disconnect) (packet.Provider, error) {
	var err error

	err = packet.CodeSuccess

	if s.version == packet.ProtocolV50 {
		// FIXME: CodeRefusedBadUsernameOrPassword has same id as CodeDisconnectWithWill
		if pkt.ReasonCode() != packet.CodeRefusedBadUsernameOrPassword {
			s.will = nil
		}

		if prop := pkt.PropertyGet(packet.PropertySessionExpiryInterval); prop != nil {
			if val, ok := prop.AsInt(); ok == nil {
				// If the Session Expiry Interval in the CONNECT packet was zero, then it is a Protocol Error to set a non-
				// zero Session Expiry Interval in the DISCONNECT packet sent by the Client. If such a non-zero Session
				// Expiry Interval is received by the Server, it does not treat it as a valid DISCONNECT packet. The Server
				// uses DISCONNECT with Reason Code 0x82 (Protocol Error) as described in section 4.13.
				if (s.expireIn != nil && *s.expireIn == 0) && val != 0 {
					err = packet.CodeProtocolError
				} else {
					s.expireIn = &val
				}
			}
		}
	} else {
		s.will = nil
	}

	return nil, err
}

// SignalOffline put subscriber in offline mode
func (s *session) SignalOffline() {
	s.subscriber.Offline(!s.durable)
}

// SignalConnectionClose net connection has been closed
func (s *session) SignalConnectionClose(params connection.DisconnectParams) {
	// If session expiry is set to 0, the Session ends when the Network Connection is closed
	if s.expireIn != nil && *s.expireIn == 0 {
		s.durable = true
	}

	// valid willMsg pointer tells we have will message
	// if session is clean send will regardless to will delay
	if s.will != nil && s.willDelay == 0 {
		if err := s.messenger.Publish(s.will); err != nil {
			s.log.Error("Publish will message", zap.String("ClientID", s.id), zap.Error(err))
		}
		s.will = nil
	}

	s.connectionClosed(s.id, params.Reason)

	if s.durable && len(params.Packets) > 0 {
		s.persistence.PacketsStore([]byte(s.id), params.Packets)
	}

	keepContainer := s.durable && s.subscriber.HasSubscriptions()

	if !s.durable || !s.subscriber.HasSubscriptions() {
		s.subscriberShutdown(s.id, s.subscriber)
		s.subscriber = nil
	}

	var exp *expiry

	if params.Reason != packet.CodeSessionTakenOver {
		if s.willDelay > 0 || (s.expireIn != nil && *s.expireIn > 0) {
			exp = newExpiry(
				expiryConfig{
					id:        s.id,
					createdAt: s.createdAt,
					messenger: s.messenger,
					will:      s.will,
					expireIn:  s.expireIn,
					willDelay: s.willDelay,
				})

			keepContainer = true
		}
	}

	s.sessionOffline(s.id, keepContainer, exp)

	s.stopReq.Do(func() {})
	s.conn = nil
}
