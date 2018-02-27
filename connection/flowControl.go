package connection

import (
	"errors"
	"sync/atomic"

	"github.com/VolantMQ/mqttp"
)

var (
	errExit          = errors.New("exit")
	errQuotaExceeded = errors.New("quota exceeded")
)

func (s *impl) flowReAcquire(id packet.IDType) {
	atomic.AddInt32(&s.txQuota, -1)
	s.flowInUse.Store(id, true)
}

func (s *impl) flowAcquire() (packet.IDType, error) {
	select {
	case <-s.quit:
		return 0, errExit
	default:
	}

	var err error
	if atomic.AddInt32(&s.txQuota, -1) == 0 {
		err = errQuotaExceeded
	}

	var id packet.IDType

	for count := 0; count <= 0xFFFF; count++ {
		s.flowCounter++
		id = packet.IDType(s.flowCounter)
		if _, ok := s.flowInUse.LoadOrStore(id, true); !ok {
			break
		}
	}

	return id, err
}

func (s *impl) flowRelease(id packet.IDType) bool {
	s.flowInUse.Delete(id)

	return atomic.AddInt32(&s.txQuota, 1) == 1
}
