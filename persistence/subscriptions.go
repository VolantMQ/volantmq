package persistence

import (
	"sync"
)

type subscriptions struct {
	status *dbStatus

	lock sync.Mutex
	subs map[string][]byte
}

func (s *subscriptions) Store(id []byte, data []byte) error {
	select {
	case <-s.status.done:
		return ErrNotOpen
	default:
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	s.subs[string(id)] = data

	return nil
}

func (s *subscriptions) Load(load func([]byte, []byte) error) error {
	select {
	case <-s.status.done:
		return ErrNotOpen
	default:
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	for id, data := range s.subs {
		load([]byte(id), data) // nolint: errcheck
	}

	return nil
}

func (s *subscriptions) Delete(id []byte) error {
	select {
	case <-s.status.done:
		return ErrNotOpen
	default:
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	delete(s.subs, string(id))

	return nil
}

func (s *subscriptions) Wipe() error {
	select {
	case <-s.status.done:
		return ErrNotOpen
	default:
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	s.subs = make(map[string][]byte)

	return nil
}
