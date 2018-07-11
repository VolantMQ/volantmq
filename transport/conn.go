package transport

import (
	"net"

	"github.com/VolantMQ/volantmq/auth"
)

// Conn is wrapper to net.Conn
// Implemented to encapsulate bytes statistic
type Conn interface {
	net.Conn
	//Start(cb netpoll.CallbackFn) error
	//Stop() error
	//Resume() error
}

//type pollDesc struct {
//	desc *netpoll.Desc
//	ePoll netpoll.EventPoll
//}

type Handler interface {
	OnConnection(Conn, *auth.Manager) error
}

//func (p *pollDesc) Start(cb netpoll.CallbackFn) error {
//	return p.ePoll.Start(p.desc, cb)
//}
//
//func (p *pollDesc) Stop() error {
//	return p.ePoll.Stop(p.desc)
//}
//
//func (p *pollDesc) Resume() error {
//	return p.ePoll.Resume(p.desc)
//}
