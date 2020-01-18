package topicstypes

import (
	"errors"
	"regexp"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/vlsubscriber"
	"github.com/VolantMQ/vlapi/vltypes"
)

const (
	// MWC is the multi-level wildcard
	MWC = "#"

	// SWC is the single level wildcard
	SWC = "+"

	// SEP is the topic level separator
	SEP = "/"
)

var (
	// TopicSubscribeRegexp regular expression that all subcriptions must be validated
	TopicSubscribeRegexp = regexp.MustCompile(`^(([^+#]*|\+)(/([^+#]*|\+))*(/#)?|#)$`)

	// TopicPublishRegexp regular expression that all publish to topic must be validated
	TopicPublishRegexp = regexp.MustCompile(`^[^#+]*$`)
)

var (
	// ErrInvalidConnectionType = errors.New("invalid connection type")
	// //ErrInvalidSubscriber      error = errors.New("service: Invalid subscriber")
	// ErrBufferNotReady = errors.New("buffer is not ready")

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
)

// Subscriber used inside each session as an object to provide to topic manager upon subscribe
type Subscriber interface {
	Publish(*mqttp.Publish, mqttp.QosType, mqttp.SubscriptionOptions, []uint32) error
	Hash() uintptr
}

// Subscribers used by topic manager to return list of subscribers matching topic
type Subscribers []Subscriber

// SubscriberInterface used by subscriber to handle messages
type SubscriberInterface interface {
	Publish(interface{}) error
	Subscribe(SubscribeReq) SubscribeResp
	UnSubscribe(UnSubscribeReq) UnSubscribeResp
	Retain(vltypes.RetainObject) error
	Retained(string) ([]*mqttp.Publish, error)
}

// Provider interface
type Provider interface {
	SubscriberInterface
	Shutdown() error
}

type SubscribeReq struct {
	Filter string
	S      Subscriber
	Params vlsubscriber.SubscriptionParams
	Chan   chan SubscribeResp
}

type SubscribeResp struct {
	// Granted  mqttp.QosType
	Params   vlsubscriber.SubscriptionParams
	Retained []*mqttp.Publish
	Err      error
}

type UnSubscribeReq struct {
	Filter string
	S      Subscriber
	Chan   chan UnSubscribeResp
}

type UnSubscribeResp struct {
	Err error
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
