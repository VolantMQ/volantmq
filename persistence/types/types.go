package persistenceTypes

import (
	"time"

	"github.com/troian/surgemq/message"
)

// Errors persistence errors
type Errors int

const (
	// ErrInvalidArgs invalid arguments provided
	ErrInvalidArgs Errors = iota
	// ErrUnknownProvider if provider is unknown
	ErrUnknownProvider
	// ErrAlreadyExists object already exists
	ErrAlreadyExists
	// ErrNotInitialized persistence provider not initialized yet
	ErrNotInitialized
	// ErrNotFound object not found
	ErrNotFound
	// ErrNotOpen storage is not open
	ErrNotOpen
)

var errorsDesc = map[Errors]string{
	ErrInvalidArgs:     "persistence: invalid arguments",
	ErrUnknownProvider: "persistence: unknown provider",
	ErrAlreadyExists:   "persistence: already exists",
	ErrNotInitialized:  "persistence: not initialized",
	ErrNotFound:        "persistence: not found",
	ErrNotOpen:         "persistence: not open",
}

// Errors description during persistence
func (e Errors) Error() string {
	if s, ok := errorsDesc[e]; ok {
		return s
	}

	return "unknown error"
}

// SessionMessages persisted session messages
type SessionMessages struct {
	OutMessages   [][]byte
	UnAckMessages [][]byte
}

// SessionWill object
type SessionWill struct {
	Delay   time.Duration
	Message []byte
}

// SessionState object
type SessionState struct {
	Timestamp string
	ExpireIn  *time.Duration
	Will      *SessionWill
	Version   message.ProtocolVersion
}

// SystemState system configuration
type SystemState struct {
	Version  string
	NodeName string
}

// Retained provider for load/store retained messages
type Retained interface {
	// Store persist retained message
	Store([][]byte) error
	// Load load retained messages
	Load() ([][]byte, error)
	// Wipe retained storage
	Wipe() error
}

// Sessions interface allows operating with sessions inside backend
type Sessions interface {
	StatesIterate(func([]byte, *SessionState) error) error
	StateStore([]byte, *SessionState) error
	StateWipe([]byte) error
	StatesWipe() error

	MessagesLoad([]byte) (*SessionMessages, error)
	MessagesStore([]byte, *SessionMessages) error
	MessageStore([]byte, []byte) error
	MessagesWipe([]byte) error

	SubscriptionsIterate(func([]byte, []byte) error) error
	SubscriptionStore([]byte, []byte) error
	SubscriptionDelete([]byte) error
	SubscriptionsWipe() error

	Delete([]byte) error
}

// System persistence state of the system configuration
type System interface {
	GetInfo() (*SystemState, error)
	SetInfo(*SystemState) error
}

// Provider interface implemented by different backends
type Provider interface {
	Sessions() (Sessions, error)
	Retained() (Retained, error)
	System() (System, error)
	Shutdown() error
}

// ProviderConfig interface implemented by every backend
type ProviderConfig interface{}
