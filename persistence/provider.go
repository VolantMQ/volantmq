package persistence

import (
	"github.com/troian/surgemq/message"
)

// Retained provider for load/store retained messages
type Retained interface {
	Load() ([]message.Provider, error)
	Store([]message.Provider) error
}

// Session provider for load/store session messages
type Session interface {
	New(id string) (StoreEntry, error)
	Store(entry StoreEntry) error
	Load() ([]SessionEntry, error)
}

// StoreEntry interface implemented by backends
type StoreEntry interface {
	Add(dir string, msg message.Provider) error
}

// Provider interface implemented by different backends
type Provider interface {
	Session() Session
	Retained() Retained
	Wipe() error
	Shutdown() error
}

// SessionEntry contains all message for given session
type SessionEntry struct {
	ID string
	In struct {
		Messages []message.Provider
	}
	Out struct {
		Messages []message.Provider
	}
}
