package connection

import (
	"errors"
	"sync/atomic"

	"github.com/VolantMQ/vlapi/mqttp"
)

var (
	errExit          = errors.New("exit")
	errQuotaExceeded = errors.New("quota exceeded")
)

func (s *impl) flowReAcquire(id mqttp.IDType) {
	atomic.AddInt32(&s.txQuota, -1)
	s.flowInUse.Store(id, true)
}

func (s *impl) flowAcquire() (mqttp.IDType, error) {
	select {
	case <-s.quit:
		return 0, errExit
	default:
	}

	var err error
	if atomic.AddInt32(&s.txQuota, -1) == 0 {
		err = errQuotaExceeded
	}

	var id mqttp.IDType

	for count := 0; count <= 0xFFFF; count++ {
		s.flowCounter++
		id = mqttp.IDType(s.flowCounter)
		if _, ok := s.flowInUse.LoadOrStore(id, true); !ok {
			break
		}
	}

	return id, err
}

func (s *impl) flowRelease(id mqttp.IDType) bool {
	s.flowInUse.Delete(id)

	return atomic.AddInt32(&s.txQuota, 1) == 1
}
