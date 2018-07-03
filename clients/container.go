package clients

import (
	"sync"
	"sync/atomic"

	"github.com/VolantMQ/volantmq/subscriber"
)

var subCount int32 = 0

// container wrap session to reduce resource usage when non clean session is disconnected
// but has active subscription and/or has expiry set
type container struct {
	lock      sync.Mutex
	rmLock    sync.RWMutex
	ses       *session
	expiry    atomic.Value
	sub       *subscriber.Type
	removable bool
	removed   bool
}

func (s *container) setRemovable(rm bool) {
	s.rmLock.Lock()
	s.removable = rm
	s.rmLock.Unlock()
}

func (s *container) acquire() {
	s.lock.Lock()
}

func (s *container) release() {
	s.lock.Unlock()
}

func (s *container) session() *session {
	defer s.rmLock.Unlock()
	s.rmLock.Lock()
	return s.ses
}

func (s *container) swap(from *container) *container {
	s.ses = from.ses

	s.ses.idLock = &s.lock

	return s
}

func (s *container) subscriber(cleanStart bool, c subscriber.Config) *subscriber.Type {
	if cleanStart && s.sub != nil {
		s.sub.Offline(true)
		s.sub = nil
	}

	if s.sub == nil {
		s.sub = subscriber.New(c)
	}

	return s.sub
}
