package topicsTypes

import (
	"errors"

	"regexp"

	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/types"
)

//var (
//	// ErrUnknownProvider unknown provider
//	ErrUnknownProvider = errors.New("Unknown provider")
//)

const (
	// MWC is the multi-level wildcard
	MWC = "#"

	// SWC is the single level wildcard
	SWC = "+"

	// SEP is the topic level separator
	//SEP = "/"

	// SYS is the starting character of the system level topics
	//SYS = "$"

	// Both wildcards
	//BWC = "#+"
)

var (
	// TopicSubscribeRegexp regular expression that all subcriptions must be validated
	TopicSubscribeRegexp = regexp.MustCompile(`^(([^+#]*|\+)(/([^+#]*|\+))*(/#)?|#)$`)

	// TopicPublishRegexp regular expression that all publish to topic must be validated
	TopicPublishRegexp = regexp.MustCompile(`^[^#+]*$`)
)

var (
	//ErrInvalidConnectionType = errors.New("invalid connection type")
	////ErrInvalidSubscriber      error = errors.New("service: Invalid subscriber")
	//ErrBufferNotReady = errors.New("buffer is not ready")

	// ErrInvalidArgs invalid arguments provided
	ErrInvalidArgs = errors.New("topics: invalid arguments")

	// ErrUnexpectedObjectType invalid arguments provided
	ErrUnexpectedObjectType = errors.New("topics: unexpected object type")

	// ErrUnknownProvider if provider is unknown
	ErrUnknownProvider = errors.New("topics: unknown provider")

	// ErrAlreadyExists object already exists
	ErrAlreadyExists = errors.New("topics: already exists")

	// ErrNotFound object not found
	ErrNotFound = errors.New("topics: not found")

	// ErrNotOpen storage is not open
	//ErrNotOpen = errors.New("not open")

	//ErrOverflow = errors.New("overflow")
)

// Subscriber used inside each session as an object to provide to topic manager upon subscribe
type Subscriber interface {
	Acquire()
	Release()
	Publish(*packet.Publish, packet.QosType, packet.SubscriptionOptions, []uint32) error
	Hash() uintptr
}

// Subscribers used by topic manager to return list of subscribers matching topic
type Subscribers []Subscriber

// Provider interface
type Provider interface {
	Subscribe(string, Subscriber, *SubscriptionParams) (packet.QosType, []*packet.Publish, error)
	UnSubscribe(string, Subscriber) error
	Publish(interface{}) error
	Retain(types.RetainObject) error
	Retained(string) ([]*packet.Publish, error)
	Close() error
}

// SubscriberInterface used by subscriber to handle messages
type SubscriberInterface interface {
	Publish(interface{}) error
	Subscribe(string, Subscriber, *SubscriptionParams) (packet.QosType, []*packet.Publish, error)
	UnSubscribe(string, Subscriber) error
	Retain(types.RetainObject) error
	Retained(string) ([]*packet.Publish, error)
}

// SubscriptionParams parameters of the subscription
type SubscriptionParams struct {
	// Subscription id
	// V5.0 ONLY
	ID uint32

	// Ops requested subscription options
	Ops packet.SubscriptionOptions

	// Granted QoS granted by topics manager
	Granted packet.QosType
}

var (
	// ErrMultiLevel multi-level wildcard
	ErrMultiLevel = errors.New("multi-level wildcard found in topic and it's not at the last level")
	// ErrInvalidSubscriber invalid subscriber object
	ErrInvalidSubscriber = errors.New("subscriber cannot be nil")
	// ErrInvalidWildcardPlus Wildcard character '+' must occupy entire topic level
	ErrInvalidWildcardPlus = errors.New("wildcard character '+' must occupy entire topic level")
	// ErrInvalidWildcardSharp Wildcard character '#' must occupy entire topic level
	ErrInvalidWildcardSharp = errors.New("wildcard character '#' must occupy entire topic level")
	// ErrInvalidWildcard Wildcard characters '#' and '+' must occupy entire topic level
	ErrInvalidWildcard = errors.New("wildcard characters '#' and '+' must occupy entire topic level")
)
