package boltdb

import (
	"errors"
	"sync"

	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/persistence/types"
	"github.com/boltdb/bolt"
)

type sessions struct {
	db *dbStatus

	// transactions that are in progress right now
	wgTx *sync.WaitGroup
	lock *sync.Mutex
}

func (s *sessions) Exists(id []byte) bool {
	err := s.db.db.View(func(tx *bolt.Tx) error {
		sessions := tx.Bucket(bucketSessions)
		if sessions == nil {
			return persistenceTypes.ErrNotInitialized
		}

		ses := sessions.Bucket(id)
		if ses == nil {
			return bolt.ErrBucketNotFound
		}

		return nil
	})

	return err == nil
}

func (s *sessions) SubscriptionsStore(id []byte, data []byte) error {
	return s.db.db.Update(func(tx *bolt.Tx) error {
		sessions := tx.Bucket(bucketSessions)
		if sessions == nil {
			return persistenceTypes.ErrNotInitialized
		}

		session, err := sessions.CreateBucketIfNotExists(id)
		if err != nil {
			return err
		}

		return session.Put(bucketSubscriptions, data)
	})
}

func (s *sessions) SubscriptionsDelete(id []byte) error {
	return s.db.db.Update(func(tx *bolt.Tx) error {
		sessions := tx.Bucket(bucketSessions)
		if sessions == nil {
			return persistenceTypes.ErrNotInitialized
		}

		session := tx.Bucket(id)
		if session == nil {
			return persistenceTypes.ErrNotFound
		}

		session.Delete(id) // nolint: errcheck
		return nil
	})
}

func boolToByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func byteToBool(v byte) bool {
	if v == 0 {
		return false
	}

	return true
}

func (s *sessions) PacketsForEach(id []byte, load func(persistenceTypes.PersistedPacket) error) error {
	return s.db.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketSessions)
		if root == nil {
			return persistenceTypes.ErrNotInitialized
		}

		session := root.Bucket(id)
		if session == nil {
			return nil
		}

		packs := session.Bucket(bucketPackets)
		if packs == nil {
			return nil
		}

		packs.ForEach(func(k, v []byte) error {
			if packet := packs.Bucket(k); packet != nil {
				pPkt := persistenceTypes.PersistedPacket{UnAck: false}

				if data := packet.Get([]byte("data")); len(data) > 0 {
					pPkt.Data = data
				} else {
					// TODO(troian): return error no data
				}

				if data := packet.Get([]byte("unAck")); len(data) > 0 {
					pPkt.UnAck = byteToBool(data[0])
				}

				if data := packet.Get([]byte("expireAt")); len(data) > 0 {
					pPkt.ExpireAt = string(data)
				}

				return load(pPkt)
			}
			return nil
		})

		return nil
	})
}

func createPacketsBucket(tx *bolt.Tx, id []byte) (*bolt.Bucket, error) {
	root, err := tx.CreateBucketIfNotExists(bucketSessions)
	if err != nil {
		return nil, err
	}

	var session *bolt.Bucket
	if session, err = root.CreateBucketIfNotExists(id); err != nil {
		return nil, err
	}

	return session.CreateBucketIfNotExists(bucketPackets)
}

func storePacket(buck *bolt.Bucket, packet persistenceTypes.PersistedPacket) error {
	id, _ := buck.NextSequence() // nolint: gas
	pBuck, err := buck.CreateBucketIfNotExists(itob64(id))
	if err != nil {
		return err
	}

	if err = pBuck.Put([]byte("data"), packet.Data); err != nil {
		return err
	}

	if err = pBuck.Put([]byte("unAck"), []byte{boolToByte(packet.UnAck)}); err != nil {
		return err
	}

	if len(packet.ExpireAt) > 0 {
		if err = pBuck.Put([]byte("expireAt"), []byte(packet.ExpireAt)); err != nil {
			return err
		}
	}

	return nil
}

func (s *sessions) PacketsStore(id []byte, packets []persistenceTypes.PersistedPacket) error {
	return s.db.db.Update(func(tx *bolt.Tx) error {
		buck, err := createPacketsBucket(tx, id)
		if err != nil {
			return err
		}

		for _, entry := range packets {
			if err = storePacket(buck, entry); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *sessions) PacketStore(id []byte, packet persistenceTypes.PersistedPacket) error {
	err := s.db.db.Update(func(tx *bolt.Tx) error {
		buck, err := createPacketsBucket(tx, id)
		if err != nil {
			return err
		}

		return storePacket(buck, packet)
	})

	return err
}

func (s *sessions) PacketsDelete(id []byte) error {
	return s.db.db.Update(func(tx *bolt.Tx) error {
		if sessions := tx.Bucket(bucketSessions); sessions != nil {
			ses := sessions.Bucket(id)
			if ses == nil {
				return persistenceTypes.ErrNotFound
			}
			ses.DeleteBucket(bucketPackets) // nolint: errcheck
		}
		return nil
	})
}

func (s *sessions) LoadForEach(load func([]byte, *persistenceTypes.SessionState) error) error {
	return s.db.db.Update(func(tx *bolt.Tx) error {
		sessions := tx.Bucket(bucketSessions)
		if sessions == nil {
			return nil
		}

		return sessions.ForEach(func(k, v []byte) error {
			session := sessions.Bucket(k)
			st := &persistenceTypes.SessionState{}

			state := session.Bucket(bucketState)
			if st == nil {
				st.Errors = append(st.Errors, errors.New("session does not have state"))
			} else {
				if v := state.Get([]byte("version")); len(v) > 0 {
					st.Version = packet.ProtocolVersion(v[0])
				} else {
					st.Errors = append(st.Errors, errors.New("protocol version not found"))
				}

				st.Timestamp = string(state.Get([]byte("timestamp")))
				st.Subscriptions = state.Get(bucketSubscriptions)

				if expire := state.Bucket(bucketExpire); expire != nil {
					st.Expire = &persistenceTypes.SessionDelays{
						Since:    string(expire.Get([]byte("since"))),
						ExpireIn: string(expire.Get([]byte("expireIn"))),
						WillIn:   string(expire.Get([]byte("willIn"))),
						WillData: expire.Get([]byte("willData")),
					}
				}
			}
			return load(k, st)
		})
	})
}

func (s *sessions) StateStore(id []byte, state *persistenceTypes.SessionState) error {
	return s.db.db.Update(func(tx *bolt.Tx) error {
		sessions := tx.Bucket(bucketSessions)
		if sessions == nil {
			return persistenceTypes.ErrNotInitialized
		}

		session, err := sessions.CreateBucketIfNotExists(id)
		if err != nil {
			return err
		}

		var st *bolt.Bucket
		st, err = session.CreateBucketIfNotExists(bucketState)
		if err != nil {
			return err
		}

		if len(state.Subscriptions) > 0 {
			if err = st.Put(bucketSubscriptions, state.Subscriptions); err != nil {
				return err
			}
		}

		if err = st.Put([]byte("timestamp"), []byte(state.Timestamp)); err != nil {
			return err
		}

		if state.Expire != nil {
			expire, err := st.CreateBucketIfNotExists(bucketExpire)
			if err != nil {
				return err
			}

			if err = expire.Put([]byte("since"), []byte(state.Expire.Since)); err != nil {
				return err
			}

			if len(state.Expire.ExpireIn) > 0 {
				if err = expire.Put([]byte("expireIn"), []byte(state.Expire.ExpireIn)); err != nil {
					return err
				}
			}

			if len(state.Expire.WillIn) > 0 {
				if err = expire.Put([]byte("willIn"), []byte(state.Expire.WillIn)); err != nil {
					return err
				}
			}
			if len(state.Expire.WillData) > 0 {
				if err = expire.Put([]byte("willData"), state.Expire.WillData); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (s *sessions) StateDelete(id []byte) error {
	return s.db.db.Update(func(tx *bolt.Tx) error {
		sessions := tx.Bucket(bucketSessions)
		if sessions == nil {
			return persistenceTypes.ErrNotInitialized
		}

		session, err := sessions.CreateBucketIfNotExists(id)
		if err != nil {
			return err
		}

		session.DeleteBucket(bucketState) // nolint: errcheck

		return nil
	})
}

func (s *sessions) Delete(id []byte) error {
	return s.db.db.Update(func(tx *bolt.Tx) error {
		if buck := tx.Bucket(bucketSessions); buck != nil {
			buck.DeleteBucket(id) // nolint: errcheck
		}

		if buck := tx.Bucket(bucketSubscriptions); buck != nil {
			buck.DeleteBucket(id) // nolint: errcheck
		}

		return nil
	})
}
