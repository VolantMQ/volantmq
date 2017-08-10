package subscriber

import (
	"sync"

	"go.uber.org/zap"

	"unsafe"

	"github.com/troian/surgemq/configuration"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/topics/types"
)

// OnlinePublish invoked when subscriber respective to sessions receive message
type OnlinePublish func(*message.PublishMessage) error

// OfflinePublish invoked when subscriber respective to sessions receive message
type OfflinePublish func(string, *message.PublishMessage)

// Provider general provider
type Provider interface {
	Subscribe(string, *SubscriptionParams) (message.QosType, []*message.PublishMessage, error)
	UnSubscribe(string) error
	SetOnlineCallback(OnlinePublish)
	Subscriptions() Subscriptions
	PutOffline(bool)
	Version() message.ProtocolVersion
}

// SessionProvider passed to session
type SessionProvider interface {
	Subscribe(string, *SubscriptionParams) (message.QosType, []*message.PublishMessage, error)
	UnSubscribe(string) error
	SetOnlineCallback(OnlinePublish)
	PutOffline(bool)
}

// SubscriptionParams parameters of the subscription
type SubscriptionParams struct {
	// Subscription id
	// V5.0 ONLY
	ID uint32

	// Requested QoS requested by subscriber
	Requested message.SubscriptionOptions

	// Granted QoS granted by topics manager
	Granted message.QosType
}

// Subscriptions contains active subscriptions with respective subscription parameters
type Subscriptions map[string]*SubscriptionParams

// Config of subscriber
type Config struct {
	// ID client id
	ID string

	// Callback to invoke when subscriber is offline
	Offline OfflinePublish

	// Topics manager
	Topics topicsTypes.SubscriberInterface

	// Version MQTT protocol version
	Version message.ProtocolVersion

	// OfflineQoS0 either queue QoS0 messages when offline or not
	OfflineQoS0 bool
}

// ProviderType the subscription object
// It is public definition only for topics tests
type ProviderType struct {
	id string

	topics topicsTypes.SubscriberInterface

	subscriptions Subscriptions

	isOnline chan struct{}

	writers struct {
		online struct {
			wg      sync.WaitGroup
			publish OnlinePublish
		}
		offline struct {
			wg      sync.WaitGroup
			publish OfflinePublish
		}
	}

	access sync.WaitGroup

	log *zap.Logger

	version     message.ProtocolVersion
	offlineQoS0 bool

	publishLock sync.RWMutex // todo: find better way
}

// New subscriber object
func New(config *Config) Provider {
	p := &ProviderType{
		id:            config.ID,
		topics:        config.Topics,
		version:       config.Version,
		log:           configuration.GetProdLogger().Named("subscriber").Named(config.ID),
		subscriptions: make(Subscriptions),
		isOnline:      make(chan struct{}),
		offlineQoS0:   config.OfflineQoS0,
	}

	close(p.isOnline)

	p.writers.offline.publish = config.Offline

	return p
}

// Hash returns address of the provider struct.
// Used by topics provider as a key to subscriber object
func (s *ProviderType) Hash() uintptr {
	return uintptr(unsafe.Pointer(s))
}

// Acquire prevent subscriber being deleted before active writes finished
func (s *ProviderType) Acquire() {
	s.access.Add(1)
}

// Release subscriber once topics provider finished write
func (s *ProviderType) Release() {
	s.access.Done()
}

// Subscriptions list active subscriptions
func (s *ProviderType) Subscriptions() Subscriptions {
	return s.subscriptions
}

// Version MQTT protocol version
func (s *ProviderType) Version() message.ProtocolVersion {
	return s.version
}

// Subscribe to given topic
func (s *ProviderType) Subscribe(topic string, params *SubscriptionParams) (message.QosType, []*message.PublishMessage, error) {
	q, r, err := s.topics.Subscribe(topic, params.Requested.QoS(), s, params.ID)

	params.Granted = q
	s.subscriptions[topic] = params

	return q, r, err
}

// UnSubscribe from given topic
func (s *ProviderType) UnSubscribe(topic string) error {
	err := s.topics.UnSubscribe(topic, s)
	delete(s.subscriptions, topic)
	return err
}

// SetOnlineCallback moves subscriber to online state
// since this moment all of publishes are forwarded to provided callback
func (s *ProviderType) SetOnlineCallback(c OnlinePublish) {
	s.writers.online.publish = c
	s.isOnline = make(chan struct{})
	s.writers.offline.wg.Wait()
}

// PutOffline put subscriber offline
// if shutdown is true it does unsubscribe from all active subscriptions
func (s *ProviderType) PutOffline(shutdown bool) {
	if shutdown {
		for topic := range s.subscriptions {
			s.topics.UnSubscribe(topic, s) // nolint: errcheck
			delete(s.subscriptions, topic)
		}
	}

	select {
	case <-s.isOnline:
	default:
		close(s.isOnline)
		// Wait all of online publishes done
		s.publishLock.Lock()
		s.writers.online.wg.Wait()
		s.publishLock.Unlock()
	}

	if shutdown {
		s.access.Wait()
		s.writers.offline.wg.Wait()
	}
}

// Publish message accordingly to subscriber state
// online: forward message to session
// offline: persist message
func (s *ProviderType) Publish(m *message.PublishMessage, grantedQoS message.QosType, ids []uint32) error {
	// message version should be same as session as encode/decode depends on it
	mP, _ := message.NewMessage(s.version, message.PUBLISH)
	msg, _ := mP.(*message.PublishMessage)

	// TODO: copy properties for V5.0
	msg.SetDup(false)
	msg.SetQoS(m.QoS())     // nolint: errcheck
	msg.SetTopic(m.Topic()) // nolint: errcheck
	msg.SetRetain(false)
	msg.SetPayload(m.Payload())

	if err := msg.PropertySet(message.PropertySubscriptionIdentifier, ids); err != nil && err != message.ErrPropertyNotFound {
	}

	if msg.QoS() != message.QoS0 {
		msg.SetPacketID(0)
	}

	switch grantedQoS {
	// If a subscribing Client has been granted maximum QoS 1 for a particular Topic Filter, then a
	// QoS 0 Application Message matching the filter is delivered to the Client at QoS 0. This means
	// that at most one copy of the message is received by the Client. On the other hand, a QoS 2
	// Message published to the same topic is downgraded by the Server to QoS 1 for delivery to the
	// Client, so that Client might receive duplicate copies of the Message.
	case message.QoS1:
		if msg.QoS() == message.QoS2 {
			msg.SetQoS(message.QoS1) // nolint: errcheck
		}

		// If the subscribing Client has been granted maximum QoS 0, then an Application Message
		// originally published as QoS 2 might get lost on the hop to the Client, but the Server should never
		// send a duplicate of that Message. A QoS 1 Message published to the same topic might either get
		// lost or duplicated on its transmission to that Client.
		//case message.QoS0:
	}

	select {
	case <-s.isOnline:
		// if session is offline forward message to persisted storage
		// only with QoS1 and QoS2 and QoS0 if set by config
		qos := msg.QoS()
		if qos != message.QoS0 || (s.offlineQoS0 && qos == message.QoS0) {
			defer s.writers.offline.wg.Done()
			s.writers.offline.wg.Add(1)
			s.writers.offline.publish(s.id, msg)
		}
	default:
		// forward message to publish queue
		defer s.writers.online.wg.Done()
		s.publishLock.RLock()
		s.writers.online.wg.Add(1)
		s.publishLock.RUnlock()
		return s.writers.online.publish(msg)
	}

	return nil
}
