package bolt

import (
	"encoding/binary"
	"errors"
	boltDB "github.com/boltdb/bolt"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence"
	"sync"
)

type impl struct {
	db *boltDB.DB

	// transactions that are in progress right now
	wgTx sync.WaitGroup
	lock sync.Mutex

	r retained
	s session
}

type storeImpl struct {
	tx *boltDB.Tx

	inBuk  *boltDB.Bucket
	outBuk *boltDB.Bucket
}

type session struct {
	db *boltDB.DB

	// transactions that are in progress right now
	wgTx *sync.WaitGroup
	lock *sync.Mutex
}

type retained struct {
	db *boltDB.DB

	// transactions that are in progress right now
	wgTx *sync.WaitGroup
	lock *sync.Mutex
}

// NewBolt allocate new persistence provider of boltDB type
func NewBolt(file string) (p persistence.Provider, err error) {
	pl := &impl{}

	if pl.db, err = boltDB.Open(file, 0600, nil); err != nil {
		return nil, err
	}

	pl.r.db = pl.db
	pl.r.wgTx = &pl.wgTx
	pl.r.lock = &pl.lock

	pl.s.db = pl.db
	pl.s.wgTx = &pl.wgTx
	pl.s.lock = &pl.lock

	p = pl
	return p, nil
}

func (p *impl) Session() persistence.Session {
	return &p.s
}

func (p *impl) Retained() persistence.Retained {
	return &p.r
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

func (p *retained) Load() ([]message.Provider, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	var err error
	var tx *boltDB.Tx

	defer func() {
		if tx != nil {
			tx.Rollback() // nolint: errcheck, gas
		}
	}()

	if tx, err = p.db.Begin(false); err != nil {
		return nil, err
	}

	var bucket *boltDB.Bucket

	if bucket = tx.Bucket([]byte("retained")); bucket == nil {
		return nil, errors.New("No retained messages")
	}

	return getFromBucket(bucket)
}

func (p *retained) Store(msg []message.Provider) (err error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	var tx *boltDB.Tx

	defer tx.Rollback() // nolint: errcheck

	tx, err = p.db.Begin(true)
	if err != nil {
		return err
	}

	var bucket *boltDB.Bucket

	if bucket, err = tx.CreateBucket([]byte("retained")); err != nil {
		return err
	}

	for _, m := range msg {
		id, _ := bucket.NextSequence() // nolint: gas

		var pb *boltDB.Bucket
		if pb, err = bucket.CreateBucket(itob64(id)); err != nil {
			return err
		}

		if err = putIntoBucket(pb, m); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// NewEntry allocate store entry
func (p *session) New(sessionID string) (se persistence.StoreEntry, err error) {
	defer func() {
		if err != nil {
			if pl, ok := se.(*storeImpl); ok && pl != nil {
				if pl.tx != nil {
					pl.tx.Rollback() // nolint: errcheck, gas
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

	if pl.inBuk, err = bucket.CreateBucket([]byte("in")); err != nil {
		return se, err
	}

	if pl.outBuk, err = bucket.CreateBucket([]byte("out")); err != nil {
		return se, err
	}

	return se, nil
}

// Store commit entry into file
func (p *session) Store(entry persistence.StoreEntry) error {
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
func (p *session) Load() ([]persistence.SessionEntry, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	entries := []persistence.SessionEntry{}

	err := p.db.View(func(tx *boltDB.Tx) error {
		return tx.ForEach(func(name []byte, bucket *boltDB.Bucket) error {
			e := persistence.SessionEntry{
				ID: string(name),
			}

			if buk := bucket.Bucket([]byte("in")); buk != nil {
				var err error
				if e.In.Messages, err = getFromBucket(buk); err != nil {
					return err
				}
			}

			if buk := bucket.Bucket([]byte("out")); buk != nil {
				var err error
				if e.Out.Messages, err = getFromBucket(buk); err != nil {
					return err
				}
			}

			entries = append(entries, e)
			return nil
		})
	})

	return entries, err
}

// AddPacket to store entry
func (p *storeImpl) Add(dir string, msg message.Provider) error {
	var bucket *boltDB.Bucket

	if dir == "in" {
		bucket = p.inBuk
	} else if dir == "out" {
		bucket = p.outBuk
	} else {
		return errors.New("Invalid dir")
	}

	var packetBucket *boltDB.Bucket
	var err error

	// This returns an error only if the Tx is closed or not writeable.
	// That can't happen in an Update() call so I ignore the error check.
	id, _ := bucket.NextSequence() // nolint: gas

	if packetBucket, err = bucket.CreateBucket(itob64(id)); err != nil {
		return err
	}

	return putIntoBucket(packetBucket, msg)
}

func (p *storeImpl) commit() (err error) {
	defer p.tx.Rollback() // nolint: errcheck

	return p.tx.Commit()
}

func getFromBucket(b *boltDB.Bucket) ([]message.Provider, error) {
	entries := []message.Provider{}

	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		packBuk := b.Bucket(k)
		// firstly get id to decide what message type this is
		tmp := packBuk.Get([]byte("type"))

		mT, err := message.Type(tmp[0]).New()
		if err != nil {
			return nil, err
		}
		err = packBuk.ForEach(func(name []byte, val []byte) error {
			var e error
			switch m := mT.(type) {
			case *message.PublishMessage:
				switch string(name) {
				case "id":
					m.SetPacketID(binary.BigEndian.Uint16(val))
				case "topic":
					e = m.SetTopic(string(val))
				case "payload":
					buf := make([]byte, len(val))
					copy(buf, val)
					m.SetPayload(buf)
				case "qos":
					e = m.SetQoS(message.QosType(val[0]))
				}
			}

			return e
		})
		if err != nil {
			return nil, err
		}

		entries = append(entries, mT)
	}

	return entries, nil
}

func putIntoBucket(b *boltDB.Bucket, msg message.Provider) error {
	if err := b.Put([]byte("type"), []byte{byte(msg.Type())}); err != nil {
		return err
	}

	if msg.PacketID() != 0 {
		if err := b.Put([]byte("id"), itob16(msg.PacketID())); err != nil {
			return err
		}
	}

	switch m := msg.(type) {
	case *message.PublishMessage:
		if err := b.Put([]byte("qos"), []byte{byte(m.QoS())}); err != nil {
			return err
		}

		if err := b.Put([]byte("topic"), []byte(m.Topic())); err != nil {
			return err
		}

		if len(m.Payload()) > 0 {
			if err := b.Put([]byte("topic"), m.Payload()); err != nil {
				return err
			}
		}
	case *message.PubRelMessage:
		// have nothing to do here
	}

	return nil
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
