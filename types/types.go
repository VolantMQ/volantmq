package types

import (
	"sync"

	"errors"

	"github.com/troian/surgemq/message"
	"go.uber.org/zap"
)

//type OnCompleteFunc func(msg, ack message.Provider, err error) error

// OnPublishFunc on publish
type OnPublishFunc func(msg *message.PublishMessage) error

// OnCompleteFunc on complete
type OnCompleteFunc func(msg, ack message.Provider, err error) error

// OnSessionClose session signal on it's close
//type OnSessionClose func(id uint64)

// Subscriber used inside each session as an object to provide to topic manager upon subscribe
type Subscriber struct {
	WgWriters sync.WaitGroup
	Publish   OnPublishFunc
}

// Subscribers used by topic manager to return list of subscribers matching topic
type Subscribers []*Subscriber

// DuplicateConfig defines behaviour of server on new client with existing ID
type DuplicateConfig struct {
	// Replace Either allow or deny replacing of existing session if there new client
	// with same clientID
	Replace bool

	// OnAttempt If requested we notify if there is attempt to dup session
	OnAttempt func(id string, replaced bool)
}

// LogInterface inherited by internal packages to provide hierarchical logs
type LogInterface struct {
	Prod *zap.Logger
	Dev  *zap.Logger
}

// Errors
var (
	ErrInvalidConnectionType error = errors.New("invalid connection type")
	//ErrInvalidSubscriber      error = errors.New("service: Invalid subscriber")
	ErrBufferNotReady error = errors.New("buffer is not ready")
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
