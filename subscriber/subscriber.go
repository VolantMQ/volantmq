package subscriber

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"go.uber.org/zap"

	"github.com/VolantMQ/mqttp"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/topics/types"
)

// SessionProvider passed to present network connection
type SessionProvider interface {
	Subscriptions() Subscriptions
	Subscribe(string, *topicsTypes.SubscriptionParams) (packet.QosType, []*packet.Publish, error)
	UnSubscribe(string) error
	HasSubscriptions() bool
	Online(c Publisher)
	Offline(bool)
	Hash() uintptr
}

type Publisher interface {
	Publish(string, *packet.Publish)
}

type publisher struct {
	Publisher
	sync.WaitGroup
}

// Subscriptions contains active subscriptions with respective subscription parameters
type Subscriptions map[string]*topicsTypes.SubscriptionParams

// Config subscriber config options
type Config struct {
	ID             string
	OfflinePublish Publisher
	Topics         topicsTypes.SubscriberInterface
	Version        packet.ProtocolVersion
}

// Type subscriber object
type Type struct {
	subscriptions Subscriptions
	publisher     atomic.Value
	log           *zap.SugaredLogger
	access        sync.WaitGroup
	inProgress    sync.WaitGroup
	Config
}

var _ SessionProvider = (*Type)(nil)

// New allocate new subscriber
func New(c Config) *Type {
	p := &Type{
		subscriptions: make(Subscriptions),
		Config:        c,
		log:           configuration.GetLogger().Named("subscriber"),
	}

	p.publisher.Store(&publisher{
		Publisher: c.OfflinePublish,
	})

	return p
}

// GetID get subscriber id
func (s *Type) GetID() string {
	return s.ID
}

// Hash returns address of the provider struct.
// Used by topics provider as a key to subscriber object
func (s *Type) Hash() uintptr {
	return uintptr(unsafe.Pointer(s))
}

// HasSubscriptions either has active subscriptions or not
func (s *Type) HasSubscriptions() bool {
	return len(s.subscriptions) != 0
}

// Acquire prevent subscriber being deleted before active writes finished
func (s *Type) Acquire() {
	s.access.Add(1)
}

// Release subscriber once topics provider finished write
func (s *Type) Release() {
	s.access.Done()
}

// GetVersion return MQTT protocol version
func (s *Type) GetVersion() packet.ProtocolVersion {
	return s.Version
}

// Subscriptions list active subscriptions
func (s *Type) Subscriptions() Subscriptions {
	return s.subscriptions
}

// Subscribe to given topic
func (s *Type) Subscribe(topic string, params *topicsTypes.SubscriptionParams) (packet.QosType, []*packet.Publish, error) {
	q, r, err := s.Topics.Subscribe(topic, s, params)

	s.subscriptions[topic] = params

	return q, r, err
}

// UnSubscribe from given topic
func (s *Type) UnSubscribe(topic string) error {
	delete(s.subscriptions, topic)
	return s.Topics.UnSubscribe(topic, s)
}

// Publish message accordingly to subscriber state
// online: forward message to session
// offline: persist message
func (s *Type) Publish(p *packet.Publish, grantedQoS packet.QosType, ops packet.SubscriptionOptions, ids []uint32) error {
	pkt, err := p.Clone(s.Version)
	if err != nil {
		return err
	}

	if len(ids) > 0 {
		if err = pkt.PropertySet(packet.PropertySubscriptionIdentifier, ids); err != nil {
			return err
		}
	}

	if !ops.RAP() {
		pkt.SetRetain(false)
	}

	if pkt.QoS() != packet.QoS0 {
		pkt.SetPacketID(0)
	}

	switch grantedQoS {
	// If a subscribing Client has been granted maximum QoS 1 for a particular Topic Filter, then a
	// QoS 0 Application Message matching the filter is delivered to the Client at QoS 0. This means
	// that at most one copy of the message is received by the Client. On the other hand, a QoS 2
	// Message published to the same topic is downgraded by the Server to QoS 1 for delivery to the
	// Client, so that Client might receive duplicate copies of the Message.
	case packet.QoS1:
		if pkt.QoS() == packet.QoS2 {
			pkt.SetQoS(packet.QoS1) // nolint: errcheck
		}

		// If the subscribing Client has been granted maximum QoS 0, then an Application Message
		// originally published as QoS 2 might get lost on the hop to the Client, but the Server should never
		// send a duplicate of that Message. A QoS 1 Message published to the same topic might either get
		// lost or duplicated on its transmission to that Client.
		// case message.QoS0:
	}

	s.inProgress.Add(1)
	pb := s.publisher.Load().(*publisher)
	pb.Publish(s.ID, pkt)
	s.inProgress.Done()

	return nil
}

// Online moves subscriber to online state
// since this moment all of publishes are forwarded to provided callback
func (s *Type) Online(c Publisher) {
	s.publisher.Store(&publisher{
		Publisher: c,
	})

	s.inProgress.Wait()
}

// Offline put session offline
// if shutdown is true it does unsubscribe from all active subscriptions
func (s *Type) Offline(shutdown bool) {
	// if session is clean then remove all remaining subscriptions
	if shutdown {
		for topic := range s.subscriptions {
			s.Topics.UnSubscribe(topic, s) // nolint: errcheck
			delete(s.subscriptions, topic)
		}
	} else {
		s.publisher.Store(&publisher{
			Publisher: s.OfflinePublish,
		})

		s.inProgress.Wait()
	}
}
