package boltdb

import (
	"sync"

	"github.com/VolantMQ/volantmq/persistence"
	"github.com/boltdb/bolt"
)

type system struct {
	db *dbStatus

	// transactions that are in progress right now
	wgTx *sync.WaitGroup
	lock *sync.Mutex
}

func (s *system) GetInfo() (*persistence.SystemState, error) {
	state := &persistence.SystemState{}

	err := s.db.db.View(func(tx *bolt.Tx) error {
		sys := tx.Bucket(bucketSystem)
		if sys == nil {
			return persistence.ErrNotInitialized
		}

		state.Version = string(sys.Get([]byte("version")))
		state.NodeName = string(sys.Get([]byte("NodeName")))

		return nil
	})

	if err != nil {
		return nil, err
	}

	return state, nil
}

func (s *system) SetInfo(state *persistence.SystemState) error {
	err := s.db.db.Update(func(tx *bolt.Tx) error {
		sys := tx.Bucket(bucketSystem)
		if sys == nil {
			return persistence.ErrNotInitialized
		}

		if e := sys.Put([]byte("version"), []byte(state.Version)); e != nil {
			return e
		}

		if e := sys.Put([]byte("NodeName"), []byte(state.NodeName)); e != nil {
			return e
		}

		return nil
	})

	return err
}
