package clients

import (
	"container/list"
	"sync"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/plugin/auth"
	"github.com/VolantMQ/vlapi/plugin/persistence"
	"github.com/VolantMQ/vlapi/subscriber"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/connection"
	"github.com/VolantMQ/volantmq/types"
	"go.uber.org/zap"
)

type sessionEvents interface {
	sessionOffline(string, bool, *expiry)
	connectionClosed(string, mqttp.ReasonCode)
	subscriberShutdown(string, vlsubscriber.IFace)
}

type sessionPreConfig struct {
	id          string
	createdAt   time.Time
	messenger   types.TopicMessenger
	conn        connection.Session
	persistence persistence.Packets
	permissions vlauth.Permissions
	username    string
}

type sessionConfig struct {
	sessionEvents
	subscriber vlsubscriber.IFace
	will       *mqttp.Publish
	expireIn   *uint32
	willDelay  uint32
	durable    bool
	version    mqttp.ProtocolVersion
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

func (t *temporaryPublish) onPublish(id string, p *mqttp.Publish) {
	if p.QoS() == mqttp.QoS0 {
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

func (s *session) configure(c sessionConfig) {
	s.sessionConfig = c

	s.conn.SetOptions(connection.AttachSession(s))
}

func (s *session) start() {
	s.idLock.Unlock()
}

func (s *session) stop(reason mqttp.ReasonCode) {
	s.stopReq.Do(func() {
		s.conn.Stop(reason)
	})
}

// SignalPublish process PUBLISH packet from client
func (s *session) SignalPublish(pkt *mqttp.Publish) error {
	pkt.SetPublishID(s.subscriber.Hash())

	// [MQTT-3.3.1.3]
	if pkt.Retain() {
		if err := s.messenger.Retain(pkt); err != nil {
			s.log.Error("Error retaining message", zap.String("ClientID", s.id), zap.Error(err))
		}

		// [MQTT-3.3.1-7]
		if pkt.QoS() == mqttp.QoS0 {
			retained := mqttp.NewPublish(s.version)
			if err := retained.SetQoS(pkt.QoS()); err != nil {
				s.log.Error("set retained QoS", zap.String("ClientID", s.id), zap.Error(err))
			}
			if err := retained.SetTopic(pkt.Topic()); err != nil {
				s.log.Error("set retained topic", zap.String("ClientID", s.id), zap.Error(err))
			}
		}
	}

	if err := s.messenger.Publish(pkt); err != nil {
		s.log.Error("Couldn't publish", zap.String("ClientID", s.id), zap.Error(err))
	}

	return nil
}

// SignalSubscribe process SUBSCRIBE packet from client
func (s *session) SignalSubscribe(pkt *mqttp.Subscribe) (mqttp.IFace, error) {
	m, _ := mqttp.New(s.version, mqttp.SUBACK)
	resp, _ := m.(*mqttp.SubAck)

	id, _ := pkt.ID()
	resp.SetPacketID(id)

	var retCodes []mqttp.ReasonCode
	var retainedPublishes []*mqttp.Publish

	pkt.RangeTopics(func(t string, ops mqttp.SubscriptionOptions) {
		reason := mqttp.CodeSuccess // nolint: ineffassign

		if err := s.permissions.ACL(s.id, s.username, t, vlauth.AccessRead); err == vlauth.StatusAllow {
			subsID := uint32(0)

			// V5.0 [MQTT-3.8.2.1.2]
			if prop := pkt.PropertyGet(mqttp.PropertySubscriptionIdentifier); prop != nil {
				if v, e := prop.AsInt(); e == nil {
					subsID = v
				}
			}

			subsParams := vlsubscriber.SubscriptionParams{
				ID:  subsID,
				Ops: ops,
			}

			if grantedQoS, retained, err := s.subscriber.Subscribe(t, &subsParams); err != nil {
				reason = mqttp.QosFailure
			} else {
				reason = mqttp.ReasonCode(grantedQoS)
				retainedPublishes = append(retainedPublishes, retained...)
			}
		} else {
			// [MQTT-3.9.3]
			if s.version == mqttp.ProtocolV50 {
				reason = mqttp.CodeNotAuthorized
			} else {
				reason = mqttp.QosFailure
			}
		}

		retCodes = append(retCodes, reason)
	})

	if err := resp.AddReturnCodes(retCodes); err != nil {
		return nil, err
	}

	// Now put retained messages into publish queue
	for _, rp := range retainedPublishes {
		if p, err := rp.Clone(s.version); err == nil {
			p.SetRetain(true)
			s.conn.Publish(s.id, p)
		} else {
			s.log.Error("Couldn't clone PUBLISH message", zap.String("ClientID", s.id), zap.Error(err))
		}
	}

	return resp, nil
}

// SignalUnSubscribe process UNSUBSCRIBE packet from client
func (s *session) SignalUnSubscribe(pkt *mqttp.UnSubscribe) (mqttp.IFace, error) {
	var retCodes []mqttp.ReasonCode

	for _, t := range pkt.Topics() {
		reason := mqttp.CodeSuccess
		if err := s.permissions.ACL(s.id, s.username, t, vlauth.AccessRead); err == vlauth.StatusAllow {
			if err = s.subscriber.UnSubscribe(t); err != nil {
				s.log.Error("Couldn't unsubscribe from topic", zap.Error(err))
				reason = mqttp.CodeNoSubscriptionExisted
			}
		} else {
			reason = mqttp.CodeNotAuthorized
		}

		retCodes = append(retCodes, reason)
	}

	m, _ := mqttp.New(s.version, mqttp.UNSUBACK)
	resp, _ := m.(*mqttp.UnSubAck)

	id, _ := pkt.ID()
	resp.SetPacketID(id)
	if err := resp.AddReturnCodes(retCodes); err != nil {
		s.log.Error("unsubscribe set return codes", zap.String("ClientID", s.id), zap.Error(err))
	}

	return resp, nil
}

// SignalDisconnect process DISCONNECT packet from client
func (s *session) SignalDisconnect(pkt *mqttp.Disconnect) (mqttp.IFace, error) {
	var err error

	err = mqttp.CodeSuccess

	if s.version == mqttp.ProtocolV50 {
		// FIXME: CodeRefusedBadUsernameOrPassword has same id as CodeDisconnectWithWill
		if pkt.ReasonCode() != mqttp.CodeRefusedBadUsernameOrPassword {
			s.will = nil
		}

		if prop := pkt.PropertyGet(mqttp.PropertySessionExpiryInterval); prop != nil {
			if val, ok := prop.AsInt(); ok == nil {
				// If the Session Expiry Interval in the CONNECT packet was zero, then it is a Protocol Error to set a non-
				// zero Session Expiry Interval in the DISCONNECT packet sent by the Client. If such a non-zero Session
				// Expiry Interval is received by the Server, it does not treat it as a valid DISCONNECT mqttp. The Server
				// uses DISCONNECT with Reason Code 0x82 (Protocol Error) as described in section 4.13.
				if (s.expireIn != nil && *s.expireIn == 0) && val != 0 {
					err = mqttp.CodeProtocolError
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

func (s *session) SignalOnline() {
	s.subscriber.Online(s.conn.Publish)
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

	keepContainer := s.durable && s.subscriber.HasSubscriptions()

	if !keepContainer {
		s.subscriberShutdown(s.id, s.subscriber)
		s.subscriber = nil
	}

	if s.durable {
		if err := s.persistence.PacketsStore([]byte(s.id), params.Packets); err != nil {
			s.log.Error("persisting packets", zap.String("clientId", s.id), zap.Error(err))
		}
	} else {
		s.persistence.PacketsDelete([]byte(s.id))
	}

	var exp *expiry

	if params.Reason != mqttp.CodeSessionTakenOver {
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
