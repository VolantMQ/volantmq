package bolt

import (
	"encoding/binary"
	"errors"
	boltDB "github.com/boltdb/bolt"
	"github.com/troian/surgemq/persistence"
	"sync"
)

type impl struct {
	db *boltDB.DB

	// transactions that are in progress right now
	wgTx sync.WaitGroup
	lock sync.Mutex
}

type storeImpl struct {
	tx *boltDB.Tx
}

// NewBolt allocate new persistence provider of boltDB type
func NewBolt(file string) (p persistence.Provider, err error) {
	pl := &impl{}

	if pl.db, err = boltDB.Open(file, 0600, nil); err != nil {
		return nil, err
	}

	p = pl
	return p, nil
}

// NewEntry allocate store entry
func (p *impl) NewEntry(sessionID string) (se persistence.StoreEntry, err error) {
	defer func() {
		if err != nil {
			if pl, ok := se.(*storeImpl); ok && pl != nil {
				if pl.tx != nil {
					pl.tx.Rollback() // nolint: errcheck
				}
			}
			se = nil
		}
	}()

	pl := &storeImpl{}

	se = pl

	if pl.tx, err = p.db.Begin(true); err != nil {
		return se, err
	}

	var bucket *boltDB.Bucket
	if bucket, err = pl.tx.CreateBucket([]byte(sessionID)); err != nil {
		return se, err
	}

	if _, err = bucket.CreateBucket([]byte("in")); err != nil {
		return se, err
	}

	if _, err = bucket.CreateBucket([]byte("out")); err != nil {
		return se, err
	}

	return se, nil
}

// Store commit entry into file
func (p *impl) Store(entry persistence.StoreEntry) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	var pl *storeImpl
	var ok bool
	if pl, ok = entry.(*storeImpl); !ok {
		return errors.New("Invalid store type")
	}

	err := pl.commit()
	p.wgTx.Done()

	return err
}

// Load all entries from db
func (p *impl) Load() (persistence.LoadEntries, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	entries := persistence.LoadEntries{}

	dirLoad := func(bucket *boltDB.Bucket, to []persistence.Message) error {
		c := bucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			packBuk := bucket.Bucket(k)
			err := packBuk.ForEach(func(name []byte, val []byte) error {
				msg := persistence.Message{}

				switch string(name) {
				case "packetID":
					id := binary.BigEndian.Uint16(val)
					msg.PacketID = &id
				case "type":
					msg.Type = val[0]
				case "topic":
					buf := string(val)
					msg.Topic = &buf
				case "payload":
					buf := make([]byte, len(val))
					copy(buf, val)
					msg.Payload = &buf
				case "qos":
					tmpVal := val[0]
					msg.QoS = &tmpVal
				}

				to = append(to, msg)
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	}

	err := p.db.View(func(tx *boltDB.Tx) error {
		return tx.ForEach(func(name []byte, bucket *boltDB.Bucket) error {
			e := persistence.LoadEntry{
				SessionID: string(name),
			}

			if buk := bucket.Bucket([]byte("in")); buk != nil {
				if err := dirLoad(buk, e.In.Messages); err != nil {
					return err
				}
			}

			if buk := bucket.Bucket([]byte("out")); buk != nil {
				if err := dirLoad(buk, e.Out.Messages); err != nil {
					return err
				}
			}

			entries = append(entries, e)
			return nil
		})
	})

	return entries, err
}

func (p *impl) Wipe() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	err := p.db.View(func(tx *boltDB.Tx) error {
		return tx.ForEach(func(name []byte, _ *boltDB.Bucket) error {
			return tx.DeleteBucket(name)
		})
	})

	return err
}

// Shutdown provider
func (p *impl) Shutdown() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.wgTx.Wait()

	err := p.db.Close()
	p.db = nil

	return err
}

// AddPacket to store entry
func (p *storeImpl) AddPacket(packet *persistence.Packet) error {
	bucket := p.tx.Bucket([]byte(packet.SessionID))
	if bucket == nil {
		return errors.New("bucket does not exist")
	}

	dirBucket := bucket.Bucket([]byte(packet.Direction))
	if dirBucket == nil {
		return errors.New("bucket does not exist")
	}

	var packetBucket *boltDB.Bucket
	var err error

	// This returns an error only if the Tx is closed or not writeable.
	// That can't happen in an Update() call so I ignore the error check.
	id, _ := dirBucket.NextSequence()

	if packetBucket, err = dirBucket.CreateBucket(itob64(id)); err != nil {
		return err
	}

	if err = packetBucket.Put([]byte("type"), []byte{packet.Type}); err != nil {
		return err
	}

	if packet.PacketID != nil {
		if err = packetBucket.Put([]byte("packetID"), itob16(*packet.PacketID)); err != nil {
			return err
		}
	}

	if packet.QoS != nil {
		if err = packetBucket.Put([]byte("qos"), []byte{*packet.QoS}); err != nil {
			return err
		}
	}

	if packet.Topic != nil {
		if err = packetBucket.Put([]byte("topic"), []byte(*packet.Topic)); err != nil {
			return err
		}
	}

	if packet.Payload != nil {
		if err = packetBucket.Put([]byte("topic"), *packet.Payload); err != nil {
			return err
		}
	}

	return nil
}

func (p *storeImpl) commit() (err error) {
	defer p.tx.Rollback() // nolint: errcheck

	return p.tx.Commit()
}

// itob returns an 8-byte big endian representation of v.
func itob64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func itob16(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}
