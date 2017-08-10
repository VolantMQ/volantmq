package types

import (
	"go.uber.org/zap"
)

// LogInterface inherited by internal packages to provide hierarchical logs
type LogInterface struct {
	Prod *zap.Logger
	Dev  *zap.Logger
}

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

// RetainObject general interface of the retain as not only publish message can be retained
type RetainObject interface {
	Topic() string
}

// TopicMessenger interface for session or systree used to publish or retain messages
type TopicMessenger interface {
	Publish(interface{}) error
	Retain(RetainObject) error
}
