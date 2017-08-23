package connection

import (
	"sync"

	"github.com/troian/surgemq/message"
)

type onRelease func(msg message.Provider)

type ackQueue struct {
	lock      sync.Mutex
	messages  map[message.PacketID]message.Provider
	onRelease onRelease
}

func newAckQueue(cb onRelease) *ackQueue {
	a := ackQueue{
		messages:  make(map[message.PacketID]message.Provider),
		onRelease: cb,
	}

	return &a
}

func (a *ackQueue) store(msg message.Provider) {
	a.lock.Lock()
	defer a.lock.Unlock()

	id, _ := msg.PacketID()

	a.messages[id] = msg
}

func (a *ackQueue) release(msg message.Provider) {
	a.lock.Lock()
	defer a.lock.Unlock()

	id, _ := msg.PacketID()

	if e, ok := a.messages[id]; ok {
		if a.onRelease != nil {
			a.onRelease(e)
		}
		a.messages[id] = nil
		delete(a.messages, id)
	}
}

func (a *ackQueue) get() map[message.PacketID]message.Provider {
	return a.messages
}

//func (a *ackQueue) wipe() {
//	a.lock.Lock()
//	defer a.lock.Unlock()
//
//	a.messages = make(map[message.PacketID]message.Provider)
//}
