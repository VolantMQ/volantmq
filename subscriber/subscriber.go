package subscriber

import (
	"unsafe"

	"sync"

	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/topics/types"
)

// ConnectionProvider passed to present network connection
type ConnectionProvider interface {
	Subscriptions() Subscriptions
	Subscribe(string, *SubscriptionParams) (message.QosType, []*message.PublishMessage, error)
	UnSubscribe(string) error
	HasSubscriptions() bool
	Online(c OnlinePublish)
	Offline(bool)
	Version() message.ProtocolVersion
}

// OnlinePublish invoked when subscriber respective to sessions receive message
type OnlinePublish func(*message.PublishMessage) error

// OfflinePublish invoked when subscriber respective to sessions receive message
type OfflinePublish func(string, *message.PublishMessage)

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

// Config subscriber config options
type Config struct {
	ID               string
	Topics           topicsTypes.SubscriberInterface
	OnOfflinePublish OfflinePublish
	OfflineQoS0      bool
	Version          message.ProtocolVersion
}

// Type subscriber object
type Type struct {
	id             string
	subscriptions  Subscriptions
	topics         topicsTypes.SubscriberInterface
	publishOffline OfflinePublish
	publishOnline  OnlinePublish
	access         sync.WaitGroup
	wgOffline      sync.WaitGroup
	wgOnline       sync.WaitGroup
	publishLock    sync.RWMutex // todo: find better way
	isOnline       chan struct{}
	offlineQoS0    bool
	version        message.ProtocolVersion
}

// New allocate new subscriber
func New(c *Config) *Type {
	p := &Type{
		isOnline:       make(chan struct{}),
		subscriptions:  make(Subscriptions),
		id:             c.ID,
		publishOffline: c.OnOfflinePublish,
		version:        c.Version,
		offlineQoS0:    c.OfflineQoS0,
		topics:         c.Topics,
	}

	close(p.isOnline)

	return p
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

// Version return MQTT protocol version
func (s *Type) Version() message.ProtocolVersion {
	return s.version
}

// Subscriptions list active subscriptions
func (s *Type) Subscriptions() Subscriptions {
	return s.subscriptions
}

// Subscribe to given topic
func (s *Type) Subscribe(topic string, params *SubscriptionParams) (message.QosType, []*message.PublishMessage, error) {
	q, r, err := s.topics.Subscribe(topic, params.Requested.QoS(), s, params.ID)

	params.Granted = q
	s.subscriptions[topic] = params

	return q, r, err
}

// UnSubscribe from given topic
func (s *Type) UnSubscribe(topic string) error {
	err := s.topics.UnSubscribe(topic, s)
	delete(s.subscriptions, topic)
	return err
}

// Publish message accordingly to subscriber state
// online: forward message to session
// offline: persist message
func (s *Type) Publish(m *message.PublishMessage, grantedQoS message.QosType, ids []uint32) error {
	// message version should be same as session as encode/decode depends on it
	mP, _ := message.NewMessage(s.version, message.PUBLISH)
	msg, _ := mP.(*message.PublishMessage)

	// TODO: copy properties for V5.0
	msg.SetDup(false)
	msg.SetQoS(m.QoS())     // nolint: errcheck
	msg.SetTopic(m.Topic()) // nolint: errcheck
	msg.SetRetain(false)
	msg.SetPayload(m.Payload())

	msg.PropertySet(message.PropertySubscriptionIdentifier, ids) // nolint: errcheck

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
			defer s.wgOffline.Done()
			s.wgOffline.Add(1)
			s.publishOffline(s.id, msg)
		}
	default:
		// forward message to publish queue
		defer s.wgOnline.Done()
		s.publishLock.RLock()
		s.wgOnline.Add(1)
		s.publishLock.RUnlock()
		return s.publishOnline(msg)
	}

	return nil
}

// Online moves subscriber to online state
// since this moment all of publishes are forwarded to provided callback
func (s *Type) Online(c OnlinePublish) {
	s.publishOnline = c
	s.isOnline = make(chan struct{})
	s.wgOffline.Wait()
}

// Offline put session offline
// if shutdown is true it does unsubscribe from all active subscriptions
func (s *Type) Offline(shutdown bool) {
	// if session is clean then remove all remaining subscriptions
	if shutdown {
		for topic := range s.subscriptions {
			s.topics.UnSubscribe(topic, s) // nolint: errcheck
			delete(s.subscriptions, topic)
		}
	}

	// wait all of remaining publishes are finished
	select {
	case <-s.isOnline:
	default:
		close(s.isOnline)
		// Wait all of online publishes done
		s.publishLock.Lock()
		s.wgOnline.Wait()
		s.publishLock.Unlock()
	}
}
