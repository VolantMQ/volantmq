package topicsTypes

import (
	"github.com/troian/surgemq/message"
	persistTypes "github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/systree"
)

// ProviderConfig interface implemented by every backend
type ProviderConfig interface{}

// MemConfig of topics manager
type MemConfig struct {
	Stat                          systree.TopicsStat
	Persist                       persistTypes.Retained
	OnCleanUnsubscribe            func([]string)
	Name                          string
	MaxQosAllowed                 message.QosType
	AllowOverlappingSubscriptions bool
}

// NewMemConfig generate default config for memory
func NewMemConfig() *MemConfig {

	return &MemConfig{
		Name:                          "mem",
		MaxQosAllowed:                 message.QoS2,
		OnCleanUnsubscribe:            func([]string) {},
		AllowOverlappingSubscriptions: false,
	}
}
