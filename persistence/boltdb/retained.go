package boltdb

import (
	"sync"

	"github.com/VolantMQ/volantmq/persistence/types"
	"github.com/boltdb/bolt"
)

type retained struct {
	db *dbStatus

	// transactions that are in progress right now
	wgTx *sync.WaitGroup
	lock *sync.Mutex
}

func (r *retained) Load() ([][]byte, error) {
	select {
	case <-r.db.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	var res [][]byte

	err := r.db.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketRetained)
		return bucket.ForEach(func(k, v []byte) error {
			buf := make([]byte, len(v))
			copy(buf, v)
			res = append(res, buf)
			return nil
		})
	})

	return res, err
}

// Store
func (r *retained) Store(data [][]byte) error {
	select {
	case <-r.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return r.db.db.Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket(bucketRetained) // nolint: errcheck
		bucket, err := tx.CreateBucket(bucketRetained)
		if err != nil {
			return err
		}

		for _, d := range data {
			id, _ := bucket.NextSequence() // nolint: gas
			if err = bucket.Put(itob64(id), d); err != nil {
				return err
			}
		}

		return nil
	})
}

// Wipe
func (r *retained) Wipe() error {
	select {
	case <-r.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return r.db.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bucketRetained); err != nil {
			return err
		}

		if _, err := tx.CreateBucket(bucketRetained); err != nil {
			return err
		}
		return nil
	})
}
