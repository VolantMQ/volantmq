package connection

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/troian/surgemq/message"
)

type packetsFlowControl struct {
	counter       uint64
	quit          chan struct{}
	cond          *sync.Cond
	lock          sync.Mutex
	inUse         map[message.PacketID]bool
	sendQuota     int32
	preserveOrder bool
}

func (s *packetsFlowControl) acquire() (message.PacketID, error) {
	defer s.lock.Unlock()
	s.lock.Lock()

	if (s.preserveOrder && !atomic.CompareAndSwapInt32(&s.sendQuota, 0, 1)) ||
		(atomic.AddInt32(&s.sendQuota, -1) == 0) {
		s.cond.Wait()
		select {
		case <-s.quit:
			return 0, errors.New("exit")
		default:
		}
	}

	var id message.PacketID

	for count := 0; count <= 0xFFFF; count++ {
		s.counter++
		id = message.PacketID(s.counter)
		if _, ok := s.inUse[id]; !ok {
			s.inUse[id] = true
			break
		}
	}

	return id, nil
}

//func (s *packetsFlowControl) reAcquire(id message.PacketID) error {
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

func (s *packetsFlowControl) release(id message.PacketID) {
	defer func() {
		atomic.AddInt32(&s.sendQuota, -1)
		s.cond.Signal()
	}()

	defer s.lock.Unlock()
	s.lock.Lock()
	delete(s.inUse, id)
}
