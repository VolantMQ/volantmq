package types

// BoltDBConfig configuration of BoltDB backend
type BoltDBConfig struct {
	File string
}

var _ ProviderConfig = (*BoltDBConfig)(nil)
