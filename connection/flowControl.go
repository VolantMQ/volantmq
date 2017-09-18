package connection

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/VolantMQ/volantmq/packet"
)

var (
	errExit          = errors.New("exit")
	errQuotaExceeded = errors.New("quota exceeded")
)

type packetsFlowControl struct {
	counter uint64
	quit    chan struct{}
	inUse   sync.Map
	quota   int32
}

func newFlowControl(quit chan struct{}, quota int32) *packetsFlowControl {
	return &packetsFlowControl{
		quit:  quit,
		quota: quota,
	}
}

func (s *packetsFlowControl) reAcquire(id packet.IDType) {
	atomic.AddInt32(&s.quota, -1)
	s.inUse.Store(id, true)
}

func (s *packetsFlowControl) acquire() (packet.IDType, error) {
	select {
	case <-s.quit:
		return 0, errExit
	default:
	}

	var err error
	if atomic.AddInt32(&s.quota, -1) == 0 {
		err = errQuotaExceeded
	}

	var id packet.IDType

	for count := 0; count <= 0xFFFF; count++ {
		s.counter++
		id = packet.IDType(s.counter)
		if _, ok := s.inUse.LoadOrStore(id, true); !ok {
			break
		}
	}

	return id, err
}

func (s *packetsFlowControl) release(id packet.IDType) bool {
	s.inUse.Delete(id)

	return atomic.AddInt32(&s.quota, 1) == 1
}
