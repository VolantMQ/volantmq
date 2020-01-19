package metrics

import (
	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/vlmonitoring"
)

type Bytes interface {
	OnSent(int)
	OnRecv(int)
}

type Packets interface {
	OnSent(p mqttp.Type)
	OnRecv(p mqttp.Type)
	OnAddRetain()
	OnSubRetain()
	OnAddUnAckSent(n int)
	OnSubUnAckSent(n int)
	OnAddUnAckRecv(n int)
	OnSubUnAckRecv(n int)
	OnAddStore(n int)
	OnSubStore(n int)
	OnRejected(n int)
}

type Subscriptions interface {
	OnSubscribe()
	OnUnsubscribe()
}

type Clients interface {
	OnConnected()
	OnDisconnected(p bool)
	OnPersisted(n uint64)
	OnRemoved(n int)
	OnExpired(n int)
	OnRejected()
}

type Informer interface {
	Bytes() Bytes
	Packets() Packets
	Subs() Subscriptions
	Clients() Clients
}

type Provider interface {
	Register(name string, face vlmonitoring.IFace) error
	Shutdown() error
}

type IFace interface {
	Informer
	Provider
}
