package clients

import (
	"sync"
	"sync/atomic"

	"github.com/VolantMQ/volantmq/subscriber"
)

type container struct {
	lock   sync.Mutex
	ses    atomic.Value
	expiry atomic.Value
	sub    *subscriber.Type
}

func (s *container) acquire() {
	s.lock.Lock()
}

func (s *container) release() {
	s.lock.Unlock()
}

func (s *container) session() *session {
	return s.ses.Load().(*session)
}

func (s *container) swap(w *container) *container {
	s.ses = w.ses

	ses := s.ses.Load().(*session)
	ses.idLock = &s.lock

	return s
}

func (s *container) subscriber(cleanStart bool, c subscriber.Config) (*subscriber.Type, bool) {
	if cleanStart && s.sub != nil {
		s.sub.Offline(true)
		s.sub = nil
	}

	if s.sub == nil {
		s.sub = subscriber.New(c)
		cleanStart = true
	} else {
		cleanStart = false
	}

	return s.sub, !cleanStart
}
