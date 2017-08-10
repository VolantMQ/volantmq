package boltdb

import (
	"sync"

	"github.com/boltdb/bolt"
	"github.com/troian/surgemq/persistence/types"
)

type sessions struct {
	db *dbStatus

	// transactions that are in progress right now
	wgTx *sync.WaitGroup
	lock *sync.Mutex
}

func (s sessions) PutOutMessage(id []byte, data []byte) error {
	err := s.db.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessions)
		if root == nil {
			return persistenceTypes.ErrNotInitialized
		}

		ses, err := root.CreateBucketIfNotExists(id)
		if err != nil {
			return err
		}

		buck, err := ses.CreateBucketIfNotExists([]byte("out"))
		if err != nil {
			return err
		}

		id, _ := buck.NextSequence()

		return buck.Put(itob64(id), data)
	})

	return err
}

// Get
func (s *sessions) Get(id []byte) (*persistenceTypes.SessionState, error) {
	select {
	case <-s.db.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	var state *persistenceTypes.SessionState

	err := s.db.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessions)
		if root == nil {
			return persistenceTypes.ErrNotInitialized
		}

		session := root.Bucket(id)
		if session == nil {
			return persistenceTypes.ErrNotFound
		}

		state = loadSession(session)
		return nil
	})

	return state, err
}

// Load
func (s *sessions) Load(load func([]byte, *persistenceTypes.SessionState)) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessions)
		if root == nil {
			return persistenceTypes.ErrNotFound
		}

		c := root.Cursor()
		for sessionID, _ := c.First(); sessionID != nil; sessionID, _ = c.Next() {
			session := root.Bucket(sessionID)

			state := loadSession(session)

			load(sessionID, state)
		}

		return nil
	})
}

func loadSession(session *bolt.Bucket) *persistenceTypes.SessionState {
	state := &persistenceTypes.SessionState{}

	loadMessages := func(buck *bolt.Bucket) [][]byte {
		res := [][]byte{}

		// nolint: errcheck, gas
		buck.ForEach(func(k, v []byte) error {
			m := make([]byte, len(v))
			copy(m, v)
			res = append(res, m)
			return nil
		})

		return res
	}

	if buck := session.Bucket([]byte("unAck")); buck != nil {
		state.UnAckMessages = loadMessages(buck)
	}

	if buck := session.Bucket([]byte("out")); buck != nil {
		state.OutMessages = loadMessages(buck)
	}

	return state
}

// Store session state
func (s *sessions) Store(id []byte, state *persistenceTypes.SessionState) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessions)
		if root == nil {
			return persistenceTypes.ErrNotFound
		}

		var err error
		if err = root.DeleteBucket(id); err != nil && err != bolt.ErrBucketNotFound {
			return err
		}

		var session *bolt.Bucket
		if session, err = root.CreateBucketIfNotExists(id); err != nil {
			return err
		}

		storeMessages := func(buck *bolt.Bucket, data [][]byte) error {
			for _, d := range data {
				id, _ := buck.NextSequence() // nolint: gas
				if e := buck.Put(itob64(id), d); e != nil {
					return e
				}
			}
			return nil
		}

		var unAckBuck *bolt.Bucket
		var outBuck *bolt.Bucket

		if unAckBuck, err = session.CreateBucketIfNotExists([]byte("unAck")); err != nil {
			return err
		}

		if outBuck, err = session.CreateBucketIfNotExists([]byte("out")); err != nil {
			return err
		}

		if err = storeMessages(unAckBuck, state.UnAckMessages); err != nil {
			return err
		}

		return storeMessages(outBuck, state.OutMessages)
	})
}

// Delete
func (s *sessions) Delete(id []byte) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		sessions := tx.Bucket(bucketSessions)
		if sessions == nil {
			return persistenceTypes.ErrNotFound
		}

		err := sessions.DeleteBucket(id)
		if err == bolt.ErrBucketNotFound {
			err = persistenceTypes.ErrNotFound
		}
		return err
	})
}

// Wipe
func (s *sessions) Wipe() error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bucketSessions); err != nil {
			return err
		}
		if _, err := tx.CreateBucket(bucketSessions); err != nil {
			return err
		}
		return nil
	})
}
