package topicsTypes

import (
	"github.com/VolantMQ/volantmq/packet"
	persistTypes "github.com/VolantMQ/volantmq/persistence/types"
	"github.com/VolantMQ/volantmq/systree"
)

// ProviderConfig interface implemented by every backend
type ProviderConfig interface{}

// MemConfig of topics manager
type MemConfig struct {
	Stat                          systree.TopicsStat
	Persist                       persistTypes.Retained
	OnCleanUnsubscribe            func([]string)
	Name                          string
	MaxQosAllowed                 packet.QosType
	AllowOverlappingSubscriptions bool
}

// NewMemConfig generate default config for memory
func NewMemConfig() *MemConfig {

	return &MemConfig{
		Name:                          "mem",
		MaxQosAllowed:                 packet.QoS2,
		OnCleanUnsubscribe:            func([]string) {},
		AllowOverlappingSubscriptions: false,
	}
}
