package persistence

// Provider interface implemented by different backends
type Provider interface {
	NewEntry(sessionID string) (StoreEntry, error)
	Load() (LoadEntries, error)
	Store(entry StoreEntry) error
	Wipe() error
	Shutdown() error
}

// Message entry used to load/store messages
type Message struct {
	PacketID *uint16
	Type     byte
	Topic    *string
	QoS      *byte
	Payload  *[]byte
}

// LoadEntry contains all message for given session
type LoadEntry struct {
	SessionID string
	In        struct {
		Messages []Message
	}
	Out struct {
		Messages []Message
	}
}

// LoadEntries just slice
type LoadEntries []LoadEntry

// StoreEntry interface implemented by backends
type StoreEntry interface {
	AddPacket(packet *Packet) error
}

// Packet object containing information how to store message
type Packet struct {
	SessionID string
	Direction string
	Message
}
