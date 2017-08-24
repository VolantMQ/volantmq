package connection

import (
	"sync"

	"github.com/troian/surgemq/packet"
)

type onRelease func(msg packet.Provider)

type ackQueue struct {
	lock      sync.Mutex
	messages  map[packet.IDType]packet.Provider
	onRelease onRelease
}

func newAckQueue(cb onRelease) *ackQueue {
	a := ackQueue{
		messages:  make(map[packet.IDType]packet.Provider),
		onRelease: cb,
	}

	return &a
}

func (a *ackQueue) store(msg packet.Provider) {
	a.lock.Lock()
	defer a.lock.Unlock()

	id, _ := msg.ID()

	a.messages[id] = msg
}

func (a *ackQueue) release(msg packet.Provider) {
	a.lock.Lock()
	defer a.lock.Unlock()

	id, _ := msg.ID()

	if e, ok := a.messages[id]; ok {
		if a.onRelease != nil {
			a.onRelease(e)
		}
		a.messages[id] = nil
		delete(a.messages, id)
	}
}

func (a *ackQueue) get() map[packet.IDType]packet.Provider {
	return a.messages
}

//func (a *ackQueue) wipe() {
//	a.lock.Lock()
//	defer a.lock.Unlock()
//
//	a.messages = make(map[message.IDType]message.Provider)
//}
