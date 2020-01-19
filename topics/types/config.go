package topicstypes

import (
	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/vlpersistence"

	"github.com/VolantMQ/volantmq/metrics"
)

// ProviderConfig interface implemented by every backend
type ProviderConfig interface{}

// MemConfig of topics manager
type MemConfig struct {
	Persist                  vlpersistence.Retained
	MetricsPackets           metrics.Packets
	MetricsSubs              metrics.Subscriptions
	OnCleanUnsubscribe       func([]string)
	Name                     string
	MaxQos                   mqttp.QosType
	OverlappingSubscriptions bool
}

// NewMemConfig generate default config for memory
func NewMemConfig() *MemConfig {
	return &MemConfig{
		Name:                     "mem",
		MaxQos:                   mqttp.QoS2,
		OnCleanUnsubscribe:       func([]string) {},
		OverlappingSubscriptions: false,
	}
}
