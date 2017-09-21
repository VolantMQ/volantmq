package persistence

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

	// ErrBrokenEntry persisted entry does not meet requirements
	ErrBrokenEntry
)

var errorsDesc = map[Errors]string{
	ErrInvalidArgs:     "persistence: invalid arguments",
	ErrUnknownProvider: "persistence: unknown provider",
	ErrAlreadyExists:   "persistence: already exists",
	ErrNotInitialized:  "persistence: not initialized",
	ErrNotFound:        "persistence: not found",
	ErrNotOpen:         "persistence: not open",
	ErrBrokenEntry:     "persistence: broken entry",
}

// Errors description during persistence
func (e Errors) Error() string {
	if s, ok := errorsDesc[e]; ok {
		return s
	}

	return "unknown error"
}

// PersistedPacket wraps packet to handle misc cases like expiration
type PersistedPacket struct {
	UnAck    bool
	ExpireAt string
	Data     []byte
}

// SessionDelays formerly known as expiry set timestamp to handle will delay and/or expiration
type SessionDelays struct {
	Since    string
	ExpireIn string
	WillIn   string
	WillData []byte
}

// SessionState object
type SessionState struct {
	Subscriptions []byte
	Timestamp     string
	Errors        []error
	Expire        *SessionDelays
	Version       byte
}

// SystemState system configuration
type SystemState struct {
	Version  string
	NodeName string
}

// Packets interface for connection to handle packets
type Packets interface {
	PacketsForEach([]byte, func(PersistedPacket) error) error
	PacketsStore([]byte, []PersistedPacket) error
}

// Subscriptions session subscriptions interface
type Subscriptions interface {
	SubscriptionsStore([]byte, []byte) error
	SubscriptionsDelete([]byte) error
}

// State session state interface
type State interface {
	StateStore([]byte, *SessionState) error
	StateDelete([]byte) error
}

// Retained provider for load/store retained messages
type Retained interface {
	// Store persist retained message
	Store([]PersistedPacket) error
	// Load load retained messages
	Load() ([]PersistedPacket, error)
	// Wipe retained storage
	Wipe() error
}

// Sessions interface allows operating with sessions inside backend
type Sessions interface {
	Packets
	Subscriptions
	State
	LoadForEach(func([]byte, *SessionState) error) error
	PacketStore([]byte, PersistedPacket) error
	PacketsDelete([]byte) error
	Exists(id []byte) bool
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
