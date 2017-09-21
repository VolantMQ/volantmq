package boltdb

import (
	"encoding/binary"
	"sync"

	"github.com/VolantMQ/volantmq/persistence/types"
	"github.com/boltdb/bolt"
)

var (
	bucketRetained      = []byte("retained")
	bucketSessions      = []byte("sessions")
	bucketSubscriptions = []byte("subscriptions")
	bucketExpire        = []byte("expire")
	bucketPackets       = []byte("packets")
	bucketState         = []byte("state")
	bucketSystem        = []byte("system")
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

	r   retained
	s   sessions
	sys system
}

var initialBuckets = [][]byte{
	bucketRetained,
	bucketSessions,
	//bucketSubscriptions,
	bucketSystem,
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

	pl.sys = system{
		db:   &pl.db,
		wgTx: &pl.wgTx,
		lock: &pl.lock,
	}

	err = pl.db.db.Update(func(tx *bolt.Tx) error {
		for _, b := range initialBuckets {
			if _, e := tx.CreateBucketIfNotExists(b); e != nil {
				return e
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	p = pl

	return p, nil
}

func (p *impl) System() (persistenceTypes.System, error) {
	select {
	case <-p.db.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	return &p.sys, nil
}

// Sessions
func (p *impl) Sessions() (persistenceTypes.Sessions, error) {
	select {
	case <-p.db.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
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

	return &p.r, nil
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
