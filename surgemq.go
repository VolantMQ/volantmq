package surgemq

import (
	"errors"
	"github.com/troian/surgemq/message"
)

// Errors
var (
	ErrInvalidConnectionType error = errors.New("service: Invalid connection type")
	//ErrInvalidSubscriber      error = errors.New("service: Invalid subscriber")
	ErrBufferNotReady error = errors.New("service: buffer is not ready")
)

// Default configs
const (
	DefaultKeepAlive        = 300 // DefaultKeepAlive default keep
	DefaultConnectTimeout   = 2   // DefaultConnectTimeout connect timeout
	DefaultAckTimeout       = 20  // DefaultAckTimeout ack timeout
	DefaultTimeoutRetries   = 3   // DefaultTimeoutRetries retries
	MinKeepAlive            = 30
	DefaultSessionsProvider = "mem"         // DefaultSessionsProvider default session provider
	DefaultAuthenticator    = "mockSuccess" // DefaultAuthenticator default auth provider
	DefaultTopicsProvider   = "mem"         // DefaultTopicsProvider default topics provider
)

type (
	// OnCompleteFunc on complete
	OnCompleteFunc func(msg, ack message.Message, err error) error
	// OnPublishFunc on publish
	OnPublishFunc func(msg *message.PublishMessage) error
)
