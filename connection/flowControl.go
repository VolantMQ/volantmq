package connection

import (
	"errors"
	"sync/atomic"

	"github.com/VolantMQ/volantmq/packet"
)

var (
	errExit          = errors.New("exit")
	errQuotaExceeded = errors.New("quota exceeded")
)

//type packetsFlowControl struct {
//	counter uint64
//	quit    chan struct{}
//	inUse   sync.Map
//	quota   int32
//}

func (s *Type) flowReAcquire(id packet.IDType) {
	atomic.AddInt32(&s.SendQuota, -1)
	s.flowInUse.Store(id, true)
}

func (s *Type) flowAcquire() (packet.IDType, error) {
	select {
	case <-s.quit:
		return 0, errExit
	default:
	}

	var err error
	if atomic.AddInt32(&s.SendQuota, -1) == 0 {
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

func (s *Type) flowRelease(id packet.IDType) bool {
	s.flowInUse.Delete(id)

	return atomic.AddInt32(&s.SendQuota, 1) == 1
}
