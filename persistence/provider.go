package persistence

// StoreEntry interface implemented by backends
type StoreEntry interface {
	Add(dir string, msg *Message) error
}

// Provider interface implemented by different backends
type Provider interface {
	SessionNew(id string) (StoreEntry, error)
	SessionStore(entry StoreEntry) error
	SessionsLoad() ([]SessionEntry, error)

	RetainedLoad() ([]*Message, error)
	RetainedStore([]*Message) error

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
