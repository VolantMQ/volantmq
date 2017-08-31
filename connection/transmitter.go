package connection

import (
	"container/list"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/troian/surgemq/packet"
	"go.uber.org/zap"
)

type transmitterConfig struct {
	id           string
	quit         chan struct{}
	flowControl  *packetsFlowControl
	pubIn        *ackQueue
	pubOut       *ackQueue
	log          *zap.Logger
	conn         net.Conn
	onDisconnect func(bool)
}

type transmitter struct {
	transmitterConfig
	lock      sync.Mutex
	messages  *list.List
	available chan int
	running   uint32
	timer     *time.Timer
	wg        sync.WaitGroup
}

func newTransmitter(config *transmitterConfig) *transmitter {
	p := &transmitter{
		transmitterConfig: *config,
		messages:          list.New(),
		available:         make(chan int, 1),
		timer:             time.NewTimer(1 * time.Second),
	}

	p.timer.Stop()

	return p
}

func (p *transmitter) shutdown() {
	p.timer.Stop()
	p.wg.Wait()
	select {
	case <-p.available:
	default:
		close(p.available)
	}
}

func (p *transmitter) loadFront(value interface{}) {
	p.lock.Lock()
	p.messages.PushFront(value)
	p.lock.Unlock()
	p.signalAvailable()
}

//func (p *transmitter) loadBack(value interface{}) {
//	p.lock.Lock()
//	p.messages.PushBack(value)
//	p.lock.Unlock()
//	p.signalAvailable()
//}

func (p *transmitter) sendPacket(pkt packet.Provider) {
	p.lock.Lock()
	p.messages.PushFront(pkt)
	p.lock.Unlock()
	p.signalAvailable()
	p.run()
}

func (p *transmitter) queuePacket(pkt packet.Provider) {
	p.lock.Lock()
	p.messages.PushBack(pkt)
	p.lock.Unlock()
	p.signalAvailable()
	p.run()
}

func (p *transmitter) signalAvailable() {
	select {
	case p.available <- 1:
	default:
	}
}

func (p *transmitter) run() {
	if atomic.CompareAndSwapUint32(&p.running, 0, 1) {
		p.wg.Wait()
		p.wg.Add(1)
		go p.routine()
	}
}

func (p *transmitter) flushBuffers(buf net.Buffers) error {
	_, e := buf.WriteTo(p.conn)
	buf = net.Buffers{}
	// todo metrics
	return e
}

func (p *transmitter) routine() {
	var err error

	defer func() {
		p.wg.Done()

		if err != nil {
			p.onDisconnect(true)
		}
	}()

	sendBuffers := net.Buffers{}
	for atomic.LoadUint32(&p.running) == 1 {
		select {
		case <-p.timer.C:
			if err = p.flushBuffers(sendBuffers); err != nil {
				atomic.StoreUint32(&p.running, 0)
				return
			}

			sendBuffers = net.Buffers{}
			p.lock.Lock()
			l := p.messages.Len()
			p.lock.Unlock()

			if l != 0 {
				p.signalAvailable()
			} else {
				atomic.StoreUint32(&p.running, 0)
				return
			}

		case <-p.available:
			p.lock.Lock()

			var elem *list.Element

			if elem = p.messages.Front(); elem == nil {
				p.lock.Unlock()
				atomic.StoreUint32(&p.running, 0)
				break
			}

			value := p.messages.Remove(p.messages.Front())

			if p.messages.Len() != 0 {
				p.signalAvailable()
			}
			p.lock.Unlock()

			var msg packet.Provider
			switch m := value.(type) {
			case *packet.Publish:
				if m.QoS() != packet.QoS0 {
					var id packet.IDType
					if id, err = p.flowControl.acquire(); err == nil {
						m.SetPacketID(id)
						p.pubOut.store(m)
					} else {
						// if acquire id returned error session is about to exit. Queue message back and get away
						p.lock.Lock()
						p.messages.PushBack(m)
						p.lock.Unlock()
						err = errors.New("exit")
						atomic.StoreUint32(&p.running, 0)
						return
					}
				}
				msg = m
			case *unacknowledgedPublish:
				msg = m.msg
				p.pubOut.store(msg)
			default:
				msg = m.(packet.Provider)
			}

			var sz int
			if sz, err = msg.Size(); err != nil {
				p.log.Error("Couldn't calculate message size", zap.String("ClientID", p.id), zap.Error(err))
				return
			}

			buf := make([]byte, sz)
			_, err = msg.Encode(buf)
			sendBuffers = append(sendBuffers, buf)

			l := len(sendBuffers)
			if l == 1 {
				p.timer.Reset(1 * time.Millisecond)
			} else if l == 5 {
				p.timer.Stop()
				if err = p.flushBuffers(sendBuffers); err != nil {
					atomic.StoreUint32(&p.running, 0)
				}

				sendBuffers = net.Buffers{}
			}
		case <-p.quit:
			err = errors.New("exit")
			atomic.StoreUint32(&p.running, 0)
			return
		}
	}
}
