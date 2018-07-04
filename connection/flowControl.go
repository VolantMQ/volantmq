package connection

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/VolantMQ/vlapi/mqttp"
)

var (
	errExit          = errors.New("exit")
	errQuotaExceeded = errors.New("quota exceeded")
)

type flow struct {
	inUse   sync.Map
	counter uint32
	quota   int32
}

func (s *flow) reAcquire(id mqttp.IDType) error {
	if atomic.AddInt32(&s.quota, -1) == 0 {
		return errQuotaExceeded
	}

	s.inUse.Store(id, true)

	return nil
}

func (s *flow) acquire() (mqttp.IDType, error) {
	//var err error
	if atomic.LoadInt32(&s.quota) == 0 {
		return mqttp.IDType(0), errQuotaExceeded
	}

	atomic.AddInt32(&s.quota, -1)
	var id mqttp.IDType

	for count := 0; count <= 0xFFFF; count++ {
		s.counter++
		id = mqttp.IDType(s.counter)

		if id == 0 {
			s.counter++
			id = mqttp.IDType(s.counter)
		}

		if _, ok := s.inUse.LoadOrStore(id, true); !ok {
			break
		}
	}

	return id, nil
}

func (s *flow) release(id mqttp.IDType) bool {
	s.inUse.Delete(id)

	return atomic.AddInt32(&s.quota, 1) == 1
}

func (s *flow) quotaAvailable() bool {
	return atomic.LoadInt32(&s.quota) > 0
}
