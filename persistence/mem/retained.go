package mem

import (
	"github.com/VolantMQ/volantmq/persistence/types"
)

type retained struct {
	status  *dbStatus
	packets []persistenceTypes.PersistedPacket
}

func (r *retained) Load() ([]persistenceTypes.PersistedPacket, error) {
	return r.packets, nil
}

// Store
func (r *retained) Store(data []persistenceTypes.PersistedPacket) error {
	r.packets = data

	return nil
}

// Wipe
func (r *retained) Wipe() error {
	r.packets = []persistenceTypes.PersistedPacket{}
	return nil
}
