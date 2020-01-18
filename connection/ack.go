package connection

import (
	"sync"

	"github.com/VolantMQ/vlapi/mqttp"
)

type onRelease func(o, n mqttp.IFace)

type ackQueue struct {
	messages  sync.Map
	onRelease onRelease
}

func (a *ackQueue) store(pkt mqttp.IFace) {
	id, _ := pkt.ID()
	a.messages.Store(id, pkt)
}

func (a *ackQueue) release(pkt mqttp.IFace) bool {
	id, _ := pkt.ID()

	if value, ok := a.messages.Load(id); ok {
		if orig, k := value.(mqttp.IFace); k && a.onRelease != nil {
			a.onRelease(orig, pkt)
		}
		a.messages.Delete(id)

		return true
	}

	return false
}
