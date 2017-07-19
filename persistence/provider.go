package persistence

import (
	"github.com/troian/surgemq/persistence/boltdb"
	"github.com/troian/surgemq/persistence/mem"
	"github.com/troian/surgemq/persistence/types"
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
