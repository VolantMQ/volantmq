package boltdb

import (
	"sync"

	"encoding/binary"
	"time"

	"github.com/boltdb/bolt"
	"github.com/troian/surgemq/persistence/types"
)

type sessions struct {
	db *dbStatus

	// transactions that are in progress right now
	wgTx *sync.WaitGroup
	lock *sync.Mutex
}

func (s *sessions) SubscriptionStore(id []byte, data []byte) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessionsSubscriptions)
		if root == nil {
			return persistenceTypes.ErrNotInitialized
		}

		return root.Put(id, data)
	})
}

func (s *sessions) SubscriptionsIterate(load func([]byte, []byte) error) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessionsSubscriptions)

		return root.ForEach(func(k, v []byte) error {
			err := load(k, v)
			return err
		})
	})
}

func (s *sessions) SubscriptionDelete(id []byte) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.db.db.Update(func(tx *bolt.Tx) error { // nolint: errcheck
		root := tx.Bucket(bucketSessionsSubscriptions)
		if root == nil {
			return persistenceTypes.ErrNotFound
		}

		root.Delete(id) // nolint: errcheck
		return nil
	})

	return nil
}

func (s *sessions) SubscriptionsWipe() error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.db.db.Update(func(tx *bolt.Tx) error { // nolint: errcheck
		tx.DeleteBucket(bucketSessionsSubscriptions) // nolint: errcheck

		_, err := tx.CreateBucket(bucketSessionsSubscriptions)
		return err
	})

	return nil
}

func (s sessions) MessageStore(id []byte, data []byte) error {
	err := s.db.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessionsMessages)
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

func (s *sessions) MessagesLoad(id []byte) (*persistenceTypes.SessionMessages, error) {
	select {
	case <-s.db.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	var state *persistenceTypes.SessionMessages

	err := s.db.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessionsMessages)
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

func (s *sessions) MessagesStore(id []byte, state *persistenceTypes.SessionMessages) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessionsMessages)
		if root == nil {
			return persistenceTypes.ErrNotFound
		}

		var err error
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

func (s *sessions) MessagesWipe(id []byte) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket(id) // nolint: errcheck

		if _, err := tx.CreateBucket(id); err != nil {
			return err
		}
		root := tx.Bucket(bucketSessionsMessages)
		if root == nil {
			return persistenceTypes.ErrNotFound
		}

		return nil
	})
}

func (s *sessions) StateStore(id []byte, state *persistenceTypes.SessionState) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessionsStates)
		if root == nil {
			return persistenceTypes.ErrNotFound
		}

		if err := root.DeleteBucket(id); err != nil && err != bolt.ErrBucketNotFound {
			return err
		}

		var err error
		var buck *bolt.Bucket
		if buck, err = root.CreateBucket(id); err != nil {
			return err
		}

		if err = buck.Put([]byte("timestamp"), []byte(state.Timestamp)); err != nil {
			return err
		}

		if state.ExpireIn != nil {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(*state.ExpireIn))

			if err = buck.Put([]byte("expireIn"), buf); err != nil {
				return err
			}
		}

		if state.Will != nil {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(state.Will.Delay))

			if err = buck.Put([]byte("willDelay"), buf); err != nil {
				return err
			}

			if err = buck.Put([]byte("willMsg"), state.Will.Message); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *sessions) StatesIterate(load func([]byte, *persistenceTypes.SessionState) error) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessionsStates)
		if root == nil {
			return persistenceTypes.ErrNotFound
		}

		c := root.Cursor()
		for sessionID, _ := c.First(); sessionID != nil; sessionID, _ = c.Next() {
			session := root.Bucket(sessionID)

			st := &persistenceTypes.SessionState{}
			if val := session.Get([]byte("expireIn")); len(val) != 0 {
				vl := time.Duration(binary.BigEndian.Uint64(val))
				st.ExpireIn = &vl
			}

			if val := session.Get([]byte("willDelay")); len(val) != 0 {
				st.Will = &persistenceTypes.SessionWill{
					Delay:   time.Duration(binary.BigEndian.Uint64(val)),
					Message: session.Get([]byte("willMsg")),
				}
			}

			st.Timestamp = string(session.Get([]byte("timestamp")))
			if err := load(sessionID, st); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *sessions) StateWipe(id []byte) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessionsStates)
		if root == nil {
			return persistenceTypes.ErrNotFound
		}

		root.DeleteBucket(id) // nolint: errcheck
		return nil
	})
}

func (s *sessions) StatesWipe() error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket(bucketSessionsStates) // nolint: errcheck

		_, err := tx.CreateBucket(bucketSessionsStates)
		return err
	})
}

func loadSession(messages *bolt.Bucket) *persistenceTypes.SessionMessages {
	state := &persistenceTypes.SessionMessages{}

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

	if buck := messages.Bucket([]byte("unAck")); buck != nil {
		state.UnAckMessages = loadMessages(buck)
	}

	if buck := messages.Bucket([]byte("out")); buck != nil {
		state.OutMessages = loadMessages(buck)
	}

	return state
}

// Delete
func (s *sessions) Delete(id []byte) error {
	select {
	case <-s.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	return s.db.db.Update(func(tx *bolt.Tx) error {
		if buck := tx.Bucket(bucketSessionsStates); buck != nil {
			buck.DeleteBucket(id) // nolint: errcheck
		}

		if buck := tx.Bucket(bucketSessionsSubscriptions); buck != nil {
			buck.DeleteBucket(id) // nolint: errcheck
		}

		if buck := tx.Bucket(bucketSessionsMessages); buck != nil {
			buck.DeleteBucket(id) // nolint: errcheck
		}

		return nil
	})
}
