package boltdb

import (
	"sync"

	"github.com/boltdb/bolt"
	"github.com/troian/surgemq/persistence/types"
)

type subscriptions struct {
	db *dbStatus

	// transactions that are in progress right now
	wgTx *sync.WaitGroup
	lock *sync.Mutex
}

func (s *subscriptions) Store(id []byte, data []byte) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSubscriptions)
		return root.Put(id, data)
	})
}

func (s *subscriptions) Load(load func([]byte, []byte) error) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSubscriptions)
		return root.ForEach(func(k, v []byte) error {
			return load(k, v)
		})
	})
}

func (s *subscriptions) Delete(id []byte) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSubscriptions)
		return root.Delete(id)
	})
}

func (s *subscriptions) Wipe() error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bucketSubscriptions); err != nil {
			return err
		}

		if _, err := tx.CreateBucket(bucketSubscriptions); err != nil {
			return err
		}
		return nil
	})
}
