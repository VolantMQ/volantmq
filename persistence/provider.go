package persistence

import (
	"github.com/troian/surgemq/persistence/boltdb"
	"github.com/troian/surgemq/persistence/types"
)

// New persistence provider
func New(config types.ProviderConfig) (types.Provider, error) {
	if config == nil {
		return nil, types.ErrInvalidArgs
	}

	switch cfg := config.(type) {
	case *types.BoltDBConfig:
		return boltdb.NewBoltDB(cfg)
	default:
		return nil, types.ErrUnknownProvider
	}
}
