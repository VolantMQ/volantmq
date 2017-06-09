package cache

import (
	"github.com/troian/surgemq/auth/cache/boltdb"
	cacheTypes "github.com/troian/surgemq/auth/cache/types"
	authTypes "github.com/troian/surgemq/auth/types"
)

func New(config cacheTypes.ProviderConfig) (cacheTypes.Provider, error) {
	if config == nil {
		return nil, authTypes.ErrInvalidArgs
	}

	switch c := config.(type) {
	case *cacheTypes.BoltDBConfig:
		return boltdb.NewBoltDB(c)
	}

	return nil, authTypes.ErrUnknownProvider
}
