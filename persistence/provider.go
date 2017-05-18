package persistence

// Retained provider for load/store retained messages
type Retained interface {
	Load() ([]*Message, error)
	Store([]*Message) error
}

// Session provider for load/store session messages
type Session interface {
	New(id string) (StoreEntry, error)
	Store(entry StoreEntry) error
	Load() ([]SessionEntry, error)
}

// StoreEntry interface implemented by backends
type StoreEntry interface {
	Add(dir string, msg *Message) error
}

// Provider interface implemented by different backends
type Provider interface {
	Session() Session
	Retained() Retained
	Wipe() error
	Shutdown() error
}

// Message entry used to load/store messages
type Message struct {
	ID      *uint16
	Type    byte
	Topic   *string
	QoS     *byte
	Payload *[]byte
}

// SessionEntry contains all message for given session
type SessionEntry struct {
	ID string
	In struct {
		Messages []*Message
	}
	Out struct {
		Messages []*Message
	}
}
