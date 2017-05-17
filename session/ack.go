package session

import (
	"errors"
	"github.com/troian/surgemq/message"
	"sync"
)

var (
	errAckDoesNotExists = errors.New("Ack does not exists")
)

type onAckComplete func(msg message.Provider, err error)

type ackQueue struct {
	lock          sync.Mutex
	messages      map[uint16]message.Provider
	onAckComplete onAckComplete
}

func newAckQueue(onAckComplete onAckComplete) *ackQueue {
	a := ackQueue{
		messages:      make(map[uint16]message.Provider),
		onAckComplete: onAckComplete,
	}

	return &a
}

func (a *ackQueue) put(msg message.Provider) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if _, ok := a.messages[msg.PacketID()]; !ok {
		a.messages[msg.PacketID()] = msg
	}
}

func (a *ackQueue) ack(msg message.Provider) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	id := msg.PacketID()

	if e, ok := a.messages[id]; ok {
		if a.onAckComplete != nil {
			a.onAckComplete(e, nil)
		}
		a.messages[id] = nil
		delete(a.messages, id)
		return nil
	}

	return errAckDoesNotExists

}

func (a *ackQueue) get() map[uint16]message.Provider {
	return a.messages
}
