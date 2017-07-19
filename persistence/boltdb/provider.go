package boltdb

import (
	"encoding/binary"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/troian/surgemq/persistence/types"
)

var (
	bucketRetained = []byte("retained")
	bucketSessions = []byte("sessions")
	//bucketMessages      = "messages"
	bucketSubscriptions = []byte("subscriptions")
)

type dbStatus struct {
	db   *bolt.DB
	done chan struct{}
}

type impl struct {
	db dbStatus

	// transactions that are in progress right now
	wgTx sync.WaitGroup
	lock sync.Mutex

	r    retained
	s    sessions
	subs subscriptions
}

// New allocate new persistence provider of boltDB type
func New(config *persistenceTypes.BoltDBConfig) (p persistenceTypes.Provider, err error) {
	pl := &impl{
		db: dbStatus{
			done: make(chan struct{}),
		},
	}

	if pl.db.db, err = bolt.Open(config.File, 0600, nil); err != nil {
		return nil, err
	}

	pl.r = retained{
		db:   &pl.db,
		wgTx: &pl.wgTx,
		lock: &pl.lock,
	}

	pl.s = sessions{
		db:   &pl.db,
		wgTx: &pl.wgTx,
		lock: &pl.lock,
	}

	pl.subs = subscriptions{
		db:   &pl.db,
		wgTx: &pl.wgTx,
		lock: &pl.lock,
	}

	err = pl.db.db.Update(func(tx *bolt.Tx) error {
		if _, e := tx.CreateBucketIfNotExists(bucketSessions); e != nil {
			return e
		}
		if _, e := tx.CreateBucketIfNotExists(bucketRetained); e != nil {
			return e
		}
		if _, e := tx.CreateBucketIfNotExists(bucketSubscriptions); e != nil {
			return e
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	p = pl

	return p, nil
}

// Sessions
func (p *impl) Sessions() (persistenceTypes.Sessions, error) {
	select {
	case <-p.db.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	err := p.db.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketSessions)

		return err
	})

	if err != nil {
		return nil, err
	}

	return &p.s, nil
}

// Retained
func (p *impl) Retained() (persistenceTypes.Retained, error) {
	select {
	case <-p.db.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	err := p.db.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketRetained)

		return err
	})

	if err != nil {
		return nil, err
	}

	return &p.r, nil
}

// Subscriptions
func (p *impl) Subscriptions() (persistenceTypes.Subscriptions, error) {
	select {
	case <-p.db.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	err := p.db.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketSubscriptions)

		return err
	})

	if err != nil {
		return nil, err
	}

	return &p.subs, nil
}

// Shutdown provider
func (p *impl) Shutdown() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	select {
	case <-p.db.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	close(p.db.done)

	p.wgTx.Wait()

	err := p.db.db.Close()
	p.db.db = nil

	return err
}

// itob64 returns an 8-byte big endian representation of v.
func itob64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
