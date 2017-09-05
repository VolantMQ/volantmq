package persistence

import (
	"github.com/VolantMQ/volantmq/persistence/boltdb"
	"github.com/VolantMQ/volantmq/persistence/mem"
	"github.com/VolantMQ/volantmq/persistence/types"
)

// New persistence provider
func New(config persistenceTypes.ProviderConfig) (persistenceTypes.Provider, error) {
	if config == nil {
		return nil, persistenceTypes.ErrInvalidArgs
	}

	switch cfg := config.(type) {
	case *persistenceTypes.BoltDBConfig:
		return boltdb.New(cfg)
	case *persistenceTypes.MemConfig:
		return mem.New(cfg)
	default:
		return nil, persistenceTypes.ErrUnknownProvider
	}
}
