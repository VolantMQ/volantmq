package boltdb

import (
	"sync"

	"github.com/VolantMQ/volantmq/persistence"
	"github.com/boltdb/bolt"
)

type retained struct {
	db *dbStatus

	// transactions that are in progress right now
	wgTx *sync.WaitGroup
	lock *sync.Mutex
}

func (r *retained) Load() ([]persistence.PersistedPacket, error) {
	var res []persistence.PersistedPacket

	err := r.db.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketRetained)

		return bucket.ForEach(func(k, v []byte) error {
			pkt := persistence.PersistedPacket{}

			if buck := bucket.Bucket(k); buck != nil {
				pkt.Data = buck.Get([]byte("data"))
				pkt.ExpireAt = string(buck.Get([]byte("expireAt")))

				res = append(res, pkt)
			}

			return nil
		})
	})

	return res, err
}

// Store
func (r *retained) Store(packets []persistence.PersistedPacket) error {
	return r.db.db.Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket(bucketRetained) // nolint: errcheck
		bucket, err := tx.CreateBucket(bucketRetained)
		if err != nil {
			return err
		}

		for _, p := range packets {
			id, _ := bucket.NextSequence() // nolint: gas
			pack, err := bucket.CreateBucketIfNotExists(itob64(id))
			if err != nil {
				return err
			}

			err = pack.Put([]byte("data"), p.Data)
			if err != nil {
				return err
			}

			err = pack.Put([]byte("expireAt"), []byte(p.ExpireAt))
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// Wipe
func (r *retained) Wipe() error {
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
