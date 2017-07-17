package topicsTypes

import (
	persistTypes "github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/systree"
)

// ProviderConfig interface implemented by every backend
type ProviderConfig interface{}

// MemConfig of topics manager
type MemConfig struct {
	Name    string
	Stat    systree.TopicsStat
	Persist persistTypes.Retained
}
