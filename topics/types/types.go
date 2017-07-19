package topicsTypes

import (
	"errors"

	//"regexp"

	"regexp"

	"github.com/troian/surgemq/message"
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

var TopicRegexp *regexp.Regexp

func init() {
	TopicRegexp = regexp.MustCompile(`^(([^+#]*|\+)(/([^+#]*|\+))*(/#)?|#)$`)
}

var (
	//ErrInvalidConnectionType = errors.New("invalid connection type")
	////ErrInvalidSubscriber      error = errors.New("service: Invalid subscriber")
	//ErrBufferNotReady = errors.New("buffer is not ready")

	// ErrInvalidArgs invalid arguments provided
	ErrInvalidArgs = errors.New("topics: invalid arguments")

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
	Publish(*message.PublishMessage, message.QosType) error
	Hash() uintptr
}

// Subscribers used by topic manager to return list of subscribers matching topic
type Subscribers []Subscriber

// Provider interface
type Provider interface {
	Subscribe(string, message.QosType, Subscriber) (message.QosType, []*message.PublishMessage, error)
	UnSubscribe(string, Subscriber) error
	//Subscribers(string, message.QosType) (Subscribers, error)
	Publish(*message.PublishMessage) error
	Retain(*message.PublishMessage) error
	Retained(string) ([]*message.PublishMessage, error)
	Close() error
}

type Topics interface {
	Subscribe(string, message.QosType, Subscriber) (message.QosType, []*message.PublishMessage, error)
	UnSubscribe(string, Subscriber) error
	//Subscribers(string, message.QosType) (Subscribers, error)
	Retain(*message.PublishMessage) error
	Retained(string) ([]*message.PublishMessage, error)
}

type Messenger interface {
	Publish(*message.PublishMessage) error
	Retain(*message.PublishMessage) error
}

var (
	// ErrMultiLevel multi-level wildcard
	ErrMultiLevel = errors.New("Multi-level wildcard found in topic and it's not at the last level")
	// ErrInvalidSubscriber invalid subscriber object
	ErrInvalidSubscriber = errors.New("Subscriber cannot be nil")
	// ErrInvalidWildcardPlus Wildcard character '+' must occupy entire topic level
	ErrInvalidWildcardPlus = errors.New("Wildcard character '+' must occupy entire topic level")
	// ErrInvalidWildcardSharp Wildcard character '#' must occupy entire topic level
	ErrInvalidWildcardSharp = errors.New("Wildcard character '#' must occupy entire topic level")
	// ErrInvalidWildcard Wildcard characters '#' and '+' must occupy entire topic level
	ErrInvalidWildcard = errors.New("Wildcard characters '#' and '+' must occupy entire topic level")
)
