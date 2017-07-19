package persistenceTypes

type Errors int

const (
	// ErrInvalidArgs invalid arguments provided
	ErrInvalidArgs Errors = iota
	// ErrUnknownProvider if provider is unknown
	ErrUnknownProvider
	// ErrAlreadyExists object already exists
	ErrAlreadyExists
	ErrNotInitialized
	// ErrNotFound object not found
	ErrNotFound
	// ErrNotOpen storage is not open
	ErrNotOpen
	ErrOverflow
)

var errorsDesc = map[Errors]string{
	ErrInvalidArgs:     "persistence: invalid arguments",
	ErrUnknownProvider: "persistence: unknown provider",
	ErrAlreadyExists:   "persistence: already exists",
	ErrNotInitialized:  "persistence: not initialized",
	ErrNotFound:        "persistence: not found",
	ErrNotOpen:         "persistence: not open",
	ErrOverflow:        "persistence: overflow",
}

// Errors description during persistence
func (e Errors) Error() string {
	if s, ok := errorsDesc[e]; ok {
		return s
	}

	return "unknown error"
}

// SessionState persisted session state
type SessionState struct {
	Timestamp     string
	OutMessages   [][]byte
	UnAckMessages [][]byte
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

// Subscriptions interface within session
type Subscriptions interface {
	Store([]byte, []byte) error
	Load(func([]byte, []byte) error) error
	Delete([]byte) error
	Wipe() error
}

// Sessions interface allows operating with sessions inside backend
type Sessions interface {
	Load(func([]byte, *SessionState)) error
	Get([]byte) (*SessionState, error)
	PutOutMessage([]byte, []byte) error
	Store([]byte, *SessionState) error
	Delete([]byte) error
	Wipe() error
}

type Session interface {
	Get([]byte) (*SessionState, error)
}

// Provider interface implemented by different backends
type Provider interface {
	Sessions() (Sessions, error)
	Subscriptions() (Subscriptions, error)
	Retained() (Retained, error)
	Shutdown() error
}

// ProviderConfig interface implemented by every backend
type ProviderConfig interface{}
