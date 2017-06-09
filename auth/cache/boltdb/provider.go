package boltdb

import (
	"github.com/boltdb/bolt"
	cacheTypes "github.com/troian/surgemq/auth/cache/types"
	authTypes "github.com/troian/surgemq/auth/types"
	"regexp"
	"sync"
)

type impl struct {
	db     *bolt.DB
	done   chan struct{}
	active sync.WaitGroup
}

var (
	bucketRoot = []byte("permissions")
)

func NewBoltDB(config *cacheTypes.BoltDBConfig) (cacheTypes.Provider, error) {
	p := &impl{
		done: make(chan struct{}),
	}

	var err error

	if p.db, err = bolt.Open(config.File, 0600, nil); err != nil {
		return nil, err
	}

	err = p.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketRoot)
		return err
	})

	if err != nil {
		p.db.Close() // nolint: errcheck
		return nil, err
	}

	return p, nil
}

func (p *impl) Update(user string, access authTypes.AccessType, regex string) error {
	select {
	case <-p.done:
		return authTypes.ErrNotOpen
	default:
	}

	p.active.Add(1)

	err := p.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketRoot)
		if root == nil {
			return authTypes.ErrInternal
		}

		userBucket, err := root.CreateBucketIfNotExists([]byte(user))
		if err != nil {
			if (err == bolt.ErrBucketNameRequired) || (err == bolt.ErrIncompatibleValue) {
				return authTypes.ErrInvalidArgs
			}
			return err
		}

		return userBucket.Put([]byte(access.Type()), []byte(regex))
	})

	p.active.Done()

	return err
}

func (p *impl) Check(user, topic string, access authTypes.AccessType) error {
	select {
	case <-p.done:
		return authTypes.ErrNotOpen
	default:
	}

	p.active.Add(1)

	err := p.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketRoot)
		if root == nil {
			return authTypes.ErrInternal
		}

		userBucket := root.Bucket([]byte(user))
		if userBucket == nil {
			return authTypes.ErrDenied
		}

		regex := userBucket.Get([]byte(access.Type()))

		r, err := regexp.Compile(string(regex))
		if err != nil {
			return authTypes.ErrDenied
		}

		if !r.Match([]byte(topic)) {
			return authTypes.ErrDenied
		}

		return nil
	})

	p.active.Done()

	return err
}

func (p *impl) Invalidate(user string) error {
	select {
	case <-p.done:
		return authTypes.ErrNotOpen
	default:
	}

	p.active.Add(1)
	err := p.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketRoot)
		if root == nil {
			return authTypes.ErrInternal
		}

		root.DeleteBucket([]byte(user))

		return nil
	})

	p.active.Done()

	return err
}

func (p *impl) Shutdown() error {
	select {
	case <-p.done:
		return authTypes.ErrNotOpen
	default:
	}

	close(p.done)

	p.active.Wait()

	err := p.db.Close()
	p.db = nil
	return err
}
