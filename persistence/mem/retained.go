package mem

import (
	"github.com/VolantMQ/volantmq/persistence/types"
)

type retained struct {
	status   *dbStatus
	messages [][]byte
}

func (r *retained) Load() ([][]byte, error) {
	select {
	case <-r.status.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	return r.messages, nil
}

// Store
func (r *retained) Store(data [][]byte) error {
	select {
	case <-r.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	r.messages = data

	return nil
}

// Wipe
func (r *retained) Wipe() error {
	select {
	case <-r.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	r.messages = [][]byte{}
	return nil
}
