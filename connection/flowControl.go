package connection

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/troian/surgemq/packet"
)

type packetsFlowControl struct {
	counter       uint64
	quit          chan struct{}
	cond          *sync.Cond
	inUse         map[packet.IDType]bool
	sendQuota     int32
	preserveOrder bool
}

func newFlowControl(quit chan struct{}, preserveOrder bool) *packetsFlowControl {
	return &packetsFlowControl{
		inUse:         make(map[packet.IDType]bool),
		cond:          sync.NewCond(new(sync.Mutex)),
		quit:          quit,
		preserveOrder: preserveOrder,
	}
}

func (s *packetsFlowControl) acquire() (packet.IDType, error) {
	defer s.cond.L.Unlock()
	s.cond.L.Lock()

	if (s.preserveOrder && !atomic.CompareAndSwapInt32(&s.sendQuota, 0, 1)) ||
		(atomic.AddInt32(&s.sendQuota, -1) == 0) {
		s.cond.Wait()
		select {
		case <-s.quit:
			return 0, errors.New("exit")
		default:
		}
	}

	var id packet.IDType

	for count := 0; count <= 0xFFFF; count++ {
		s.counter++
		id = packet.IDType(s.counter)
		if _, ok := s.inUse[id]; !ok {
			s.inUse[id] = true
			break
		}
	}

	return id, nil
}

//func (s *packetsFlowControl) reAcquire(id message.IDType) error {
//	defer s.lock.Unlock()
//	s.lock.Lock()
//
//	if (s.preserveOrder && !atomic.CompareAndSwapInt32(&s.sendQuota, 0, 1)) ||
//		(atomic.AddInt32(&s.sendQuota, -1) == 0) {
//		s.cond.Wait()
//		select {
//		case <-s.quit:
//			return errors.New("exit")
//		default:
//		}
//	}
//
//	s.inUse[id] = true
//
//	return nil
//}

func (s *packetsFlowControl) release(id packet.IDType) {
	defer func() {
		atomic.AddInt32(&s.sendQuota, -1)
		s.cond.Signal()
	}()

	defer s.cond.L.Unlock()
	s.cond.L.Lock()
	delete(s.inUse, id)
}
