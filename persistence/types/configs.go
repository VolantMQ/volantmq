package persistenceTypes

// BoltDBConfig configuration of the BoltDB backend
type BoltDBConfig struct {
	File string
}

// MemConfig configuration of the in memory backend
type MemConfig struct{}

var _ ProviderConfig = (*BoltDBConfig)(nil)
var _ ProviderConfig = (*MemConfig)(nil)
