package types

import (
	"errors"

	"github.com/troian/surgemq/message"
)

var (
	// ErrInvalidArgs invalid arguments provided
	ErrInvalidArgs = errors.New("invalid arguments")

	// ErrUnknownProvider if provider is unknown
	ErrUnknownProvider = errors.New("Unknown provider")

	// ErrAlreadyExists object already exists
	ErrAlreadyExists = errors.New("Already exists")

	// ErrNotFound object not found
	ErrNotFound = errors.New("Not found")

	// ErrNotOpen storage is not open
	ErrNotOpen = errors.New("not open")
)

// Retained provider for load/store retained messages
type Retained interface {
	Load() ([]message.Provider, error)
	Store([]message.Provider) error
	Delete() error
}

// Subscriptions interface within session
type Subscriptions interface {
	Add(s message.TopicQos) error
	Get() (message.TopicQos, error)
	Delete() error
}

// SessionMessages contains all message for given session
type SessionMessages struct {
	In struct {
		Messages []message.Provider
	}
	Out struct {
		Messages []message.Provider
	}
}

// Messages interface within session
type Messages interface {
	Store(dir string, msg []message.Provider) error
	Load() (*SessionMessages, error)
	Delete() error
}

// Session object inside backend
type Session interface {
	Subscriptions() (Subscriptions, error)
	Messages() (Messages, error)
	ID() (string, error)
}

// Sessions interface allows operating with sessions inside backend
type Sessions interface {
	New(id string) (Session, error)
	Get(id string) (Session, error)
	GetAll() ([]Session, error)
	Delete(id string) error
}

// Provider interface implemented by different backends
type Provider interface {
	Sessions() (Sessions, error)
	Retained() (Retained, error)
	Shutdown() error
}

// ProviderConfig interface implemented by every backend
type ProviderConfig interface{}
