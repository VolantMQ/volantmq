package connection

import (
	"sync"

	"github.com/VolantMQ/volantmq/packet"
)

type onRelease func(o, n packet.Provider)

type ackQueue struct {
	messages  sync.Map
	onRelease onRelease
	//quota     int32
}

func (a *ackQueue) store(pkt packet.Provider) bool {
	//if a.quota == 0 {
	//	return false
	//}
	//
	//a.quota--
	id, _ := pkt.ID()
	a.messages.Store(id, pkt)

	return true
}

func (a *ackQueue) release(pkt packet.Provider) {
	id, _ := pkt.ID()

	//a.quota++

	if value, ok := a.messages.Load(id); ok {
		if orig, k := value.(packet.Provider); k && a.onRelease != nil {
			a.onRelease(orig, pkt)
		}
		a.messages.Delete(id)
	}
}
