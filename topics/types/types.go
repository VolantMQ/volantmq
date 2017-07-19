package topicsTypes

import (
	"errors"

	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/types"
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

// Provider interface
type Provider interface {
	Subscribe(topic string, qos message.QosType, subscriber *types.Subscriber) (message.QosType, error)
	UnSubscribe(topic string, subscriber *types.Subscriber) error
	Subscribers(topic string, qos message.QosType, subs *types.Subscribers) error
	Publish(msg *message.PublishMessage) error
	Retain(msg *message.PublishMessage) error
	Retained(topic string) ([]*message.PublishMessage, error)
	Close() error
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
