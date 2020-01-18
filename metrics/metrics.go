package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/vlmonitoring"
)

type bytes vlmonitoring.Bytes
type packets vlmonitoring.Packets
type subs vlmonitoring.Subscriptions
type clients vlmonitoring.Clients

var _ Bytes = (*bytes)(nil)
var _ Packets = (*packets)(nil)
var _ Clients = (*clients)(nil)

type stats struct {
	bytes   bytes
	packets packets
	clients clients
	subs    subs
}

type listener struct {
	orig  *stats
	prev  vlmonitoring.Stats
	timer *time.Timer
	iface vlmonitoring.IFace
}

type impl struct {
	stats
	quit      chan struct{}
	listeners struct {
		wgStarted sync.WaitGroup
		wgStopped sync.WaitGroup

		handler map[string]*listener
	}
}

var _ IFace = (*impl)(nil)

func New() IFace {
	return &impl{
		quit: make(chan struct{}),
		listeners: struct {
			wgStarted sync.WaitGroup
			wgStopped sync.WaitGroup
			handler   map[string]*listener
		}{handler: make(map[string]*listener)},
	}
}

func (im *impl) Register(name string, face vlmonitoring.IFace) error {
	ls := &listener{
		orig:  &im.stats,
		prev:  vlmonitoring.Stats{},
		iface: face,
	}

	im.listeners.handler[name] = ls

	im.listeners.wgStarted.Add(1)
	im.listeners.wgStopped.Add(1)
	ls.timer = time.NewTimer(time.Millisecond)

	go im.publisher(ls)

	im.listeners.wgStarted.Wait()

	return nil
}

func (im *impl) Shutdown() error {
	close(im.quit)

	for _, l := range im.listeners.handler {
		_ = l.iface.Shutdown()
	}

	im.listeners.wgStopped.Wait()

	for _, l := range im.listeners.handler {
		if !l.timer.Stop() {
			<-l.timer.C
		}
	}

	return nil
}

func (im *impl) Bytes() Bytes {
	return &im.bytes
}

func (im *impl) Packets() Packets {
	return &im.packets
}

func (im *impl) Clients() Clients {
	return &im.clients
}

func (im *impl) Subs() Subscriptions {
	return &im.subs
}

func (t *bytes) OnSent(n int) {
	t.Sent.AddU64(uint64(n))
}

func (t *bytes) OnRecv(n int) {
	t.Recv.AddU64(uint64(n))
}

// nolint dupl
func (t *packets) OnSent(mt mqttp.Type) {
	t.Total.Sent.AddU64(1)

	switch mt {
	case mqttp.CONNACK:
		t.ConnAck.AddU64(1)
	case mqttp.PUBLISH:
		t.Publish.Sent.AddU64(1)
	case mqttp.PUBACK:
		t.Puback.Sent.AddU64(1)
	case mqttp.PUBREC:
		t.Pubrec.Sent.AddU64(1)
	case mqttp.PUBREL:
		t.Pubrel.Sent.AddU64(1)
	case mqttp.PUBCOMP:
		t.Pubcomp.Sent.AddU64(1)
	case mqttp.SUBACK:
		t.SubAck.AddU64(1)
	case mqttp.UNSUBACK:
		t.UnSubAck.AddU64(1)
	case mqttp.PINGRESP:
		t.PingResp.AddU64(1)
	case mqttp.DISCONNECT:
		t.Disconnect.Sent.AddU64(1)
	case mqttp.AUTH:
		t.Auth.Sent.AddU64(1)
	}
}

// nolint dupl
func (t *packets) OnRecv(mt mqttp.Type) {
	t.Total.Recv.AddU64(1)

	switch mt {
	case mqttp.CONNECT:
		t.Connect.AddU64(1)
	case mqttp.PUBLISH:
		t.Publish.Recv.AddU64(1)
	case mqttp.PUBACK:
		t.Puback.Recv.AddU64(1)
	case mqttp.PUBREC:
		t.Pubrec.Recv.AddU64(1)
	case mqttp.PUBREL:
		t.Pubrel.Recv.AddU64(1)
	case mqttp.PUBCOMP:
		t.Pubcomp.Recv.AddU64(1)
	case mqttp.SUBSCRIBE:
		t.Sub.AddU64(1)
	case mqttp.UNSUBSCRIBE:
		t.UnSub.AddU64(1)
	case mqttp.PINGREQ:
		t.PingReq.AddU64(1)
	case mqttp.DISCONNECT:
		t.Disconnect.Recv.AddU64(1)
	case mqttp.AUTH:
		t.Auth.Recv.AddU64(1)
	}
}

func (t *packets) OnRejected(n int) {
	t.Rejected.AddU64(uint64(n))
}

func (t *packets) OnAddRetain() {
	t.Retained.AddU64(1)
}

func (t *packets) OnSubRetain() {
	t.Retained.SubU64(1)
}

func (t *packets) OnAddUnAckSent(n int) {
	t.UnAckSent.AddU64(uint64(n))
}

func (t *packets) OnSubUnAckSent(n int) {
	t.UnAckSent.SubU64(uint64(n))
}

func (t *packets) OnAddUnAckRecv(n int) {
	t.UnAckRecv.AddU64(uint64(n))
}

func (t *packets) OnSubUnAckRecv(n int) {
	t.UnAckRecv.SubU64(uint64(n))
}

func (t *packets) OnAddStore(n int) {
	t.Stored.AddU64(uint64(n))
}

func (t *packets) OnSubStore(n int) {
	t.Stored.SubU64(uint64(n))
}

func (t *clients) OnConnected() {
	t.Connected.AddU64(1)
	t.Total.AddU64(1)
}

func (t *clients) OnDisconnected(p bool) {
	t.Connected.SubU64(1)
	if p {
		t.Persisted.AddU64(1)
	} else {
		t.Total.SubU64(1)
	}
}

func (t *clients) OnPersisted(n uint64) {
	t.Persisted.AddU64(n)
	t.Total.AddU64(n)
	fmt.Println("persisted")
}

func (t *clients) OnRemoved(n int) {
	t.Persisted.SubU64(uint64(n))
	t.Total.SubU64(uint64(n))
}

func (t *clients) OnRejected() {
	t.Rejected.AddU64(1)
}

func (t *clients) OnExpired(n int) {
	t.Expired.AddU64(uint64(n))
}

func (t *subs) OnSubscribe() {
	t.Total.AddU64(1)
}

func (t *subs) OnUnsubscribe() {
	t.Total.SubU64(1)
}

func (im *impl) publisher(ls *listener) {
	defer im.listeners.wgStopped.Done()
	im.listeners.wgStarted.Done()

	for {
		select {
		case <-ls.timer.C:
			var st vlmonitoring.Stats
			st.Bytes.FlowCounter = ls.orig.bytes.Diff(&ls.prev.Bytes.FlowCounter)

			st.Subs.Total = ls.orig.subs.Total.Load()

			st.Packets.Total = ls.orig.packets.Total.Diff(&ls.prev.Packets.Total)
			st.Packets.Connect = ls.orig.packets.Connect.Diff(&ls.prev.Packets.Connect)
			st.Packets.ConnAck = ls.orig.packets.ConnAck.Diff(&ls.prev.Packets.ConnAck)
			st.Packets.Publish = ls.orig.packets.Publish.Diff(&ls.prev.Packets.Publish)
			st.Packets.Puback = ls.orig.packets.Puback.Diff(&ls.prev.Packets.Puback)
			st.Packets.Pubrec = ls.orig.packets.Pubrec.Diff(&ls.prev.Packets.Pubrec)
			st.Packets.Pubrel = ls.orig.packets.Pubrel.Diff(&ls.prev.Packets.Pubrel)
			st.Packets.Pubcomp = ls.orig.packets.Pubcomp.Diff(&ls.prev.Packets.Pubcomp)
			st.Packets.Sub = ls.orig.packets.Sub.Diff(&ls.prev.Packets.Sub)
			st.Packets.SubAck = ls.orig.packets.SubAck.Diff(&ls.prev.Packets.SubAck)
			st.Packets.UnSub = ls.orig.packets.UnSub.Diff(&ls.prev.Packets.UnSub)
			st.Packets.UnSubAck = ls.orig.packets.UnSubAck.Diff(&ls.prev.Packets.UnSubAck)
			st.Packets.PingReq = ls.orig.packets.PingReq.Diff(&ls.prev.Packets.PingReq)
			st.Packets.PingResp = ls.orig.packets.PingResp.Diff(&ls.prev.Packets.PingResp)
			st.Packets.Disconnect = ls.orig.packets.Disconnect.Diff(&ls.prev.Packets.Disconnect)
			st.Packets.Auth = ls.orig.packets.Auth.Diff(&ls.prev.Packets.Auth)
			st.Packets.UnAckSent = ls.orig.packets.UnAckSent.Load()
			st.Packets.UnAckRecv = ls.orig.packets.UnAckRecv.Load()
			st.Packets.Retained = ls.orig.packets.Retained.Load()

			st.Clients.Connected = ls.orig.clients.Connected.Load()
			st.Clients.Persisted = ls.orig.clients.Persisted.Load()
			st.Clients.Total = ls.orig.clients.Total.Load()

			st.Clients.Rejected = ls.orig.clients.Rejected.Diff(&ls.prev.Clients.Rejected)
			st.Clients.Expired = ls.orig.clients.Expired.Diff(&ls.prev.Clients.Expired)

			ls.iface.Push(st)

			ls.timer.Reset(time.Second)
		case <-im.quit:
			return
		}
	}
}
