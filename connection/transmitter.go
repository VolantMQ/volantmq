package connection

import (
	"container/list"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"reflect"

	"math/rand"

	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/types"
	"go.uber.org/zap"
)

type transmitterConfig struct {
	id            string
	quit          chan struct{}
	flowControl   *packetsFlowControl
	pubIn         *ackQueue
	pubOut        *ackQueue
	log           *zap.Logger
	conn          net.Conn
	onDisconnect  func(bool, error)
	maxPacketSize uint32
	topicAliasMax uint16
}

type transmitter struct {
	transmitterConfig
	available         chan int
	timer             *time.Timer
	gMessages         *list.List
	qMessages         *list.List
	wg                sync.WaitGroup
	onStop            types.OnceWait
	gLock             sync.Mutex
	qLock             sync.Mutex
	topicAlias        map[string]uint16
	running           uint32
	topicAliasCurrMax uint16
	qExceeded         bool
}

func newTransmitter(config *transmitterConfig) *transmitter {
	p := &transmitter{
		transmitterConfig: *config,
		available:         make(chan int, 1),
		timer:             time.NewTimer(1 * time.Second),
		topicAlias:        make(map[string]uint16),
		gMessages:         list.New(),
		qMessages:         list.New(),
	}
	p.timer.Stop()

	return p
}

func (p *transmitter) shutdown() {
	p.onStop.Do(func() {
		atomic.StoreUint32(&p.running, 0)
		p.timer.Stop()
		p.wg.Wait()

		select {
		case <-p.available:
		default:
			close(p.available)
		}
	})
}

func (p *transmitter) gPush(pkt packet.Provider) {
	p.gLock.Lock()
	p.gMessages.PushBack(pkt)
	p.gLock.Unlock()
	p.signalAvailable()
	p.run()
}

func (p *transmitter) gLoad(pkt packet.Provider) {
	p.gMessages.PushBack(pkt)
	p.signalAvailable()
}

func (p *transmitter) gLoadList(l *list.List) {
	p.gMessages.PushBackList(l)
	p.signalAvailable()
}

func (p *transmitter) qPush(pkt interface{}) {
	p.qLock.Lock()
	p.qMessages.PushBack(pkt)
	p.qLock.Unlock()
	p.signalAvailable()
	p.run()
}

func (p *transmitter) qLoad(pkt interface{}) {
	p.qMessages.PushBack(pkt)
	p.signalAvailable()
}

func (p *transmitter) qLoadList(l *list.List) {
	p.qMessages.PushBackList(l)
	p.signalAvailable()
}

func (p *transmitter) signalAvailable() {
	select {
	case <-p.quit:
		return
	default:
		select {
		case p.available <- 1:
		default:
		}
	}
}

func (p *transmitter) signalQuota() {
	p.qLock.Lock()
	p.qExceeded = false
	l := p.qMessages.Len()
	p.qLock.Unlock()

	if l > 0 {
		p.signalAvailable()
		p.run()
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

func (p *transmitter) packetSize(value interface{}) (int, bool) {
	var sz int
	var err error
	if obj, ok := value.(sizeAble); !ok {
		p.log.Fatal("Object does not belong to allowed types",
			zap.String("ClientID", p.id),
			zap.String("Type", reflect.TypeOf(value).String()))
	} else {
		if sz, err = obj.Size(); err != nil {
			p.log.Error("Couldn't calculate message size", zap.String("ClientID", p.id), zap.Error(err))
			return 0, false
		}
	}

	// ignore any packet with size bigger than negotiated
	if sz > int(p.maxPacketSize) {
		p.log.Warn("Ignore packet with size bigger than negotiated with client",
			zap.String("ClientID", p.id),
			zap.Uint32("negotiated", p.maxPacketSize),
			zap.Int("actual", sz))
		return 0, false
	}

	return sz, true
}

func (p *transmitter) gAvailable() bool {
	defer p.gLock.Unlock()
	p.gLock.Lock()

	return p.gMessages.Len() > 0
}

func (p *transmitter) qAvailable() bool {
	defer p.qLock.Unlock()
	p.qLock.Lock()

	return !p.qExceeded && p.qMessages.Len() > 0
}

func (p *transmitter) gPopPacket() packet.Provider {
	defer p.gLock.Unlock()
	p.gLock.Lock()

	var elem *list.Element

	if elem = p.gMessages.Front(); elem == nil {
		return nil
	}

	value := p.gMessages.Remove(elem)

	return value.(packet.Provider)
}

func (p *transmitter) qPopPacket() packet.Provider {
	defer p.qLock.Unlock()
	p.qLock.Lock()

	if elem := p.qMessages.Front(); !p.qExceeded && elem != nil {
		var pkt packet.Provider
		value := elem.Value
		switch m := value.(type) {
		case *packet.Publish:
			// try acquire packet id
			id, err := p.flowControl.acquire()
			if err == errExit {
				atomic.StoreUint32(&p.running, 0)
				return nil
			}

			if err == errQuotaExceeded {
				p.qExceeded = true
			}

			m.SetPacketID(id)
			pkt = m
		case *unacknowledgedPublish:
			pkt = m.msg

		}
		p.qMessages.Remove(elem)
		p.pubOut.store(pkt)

		return pkt
	}

	return nil
}

func (p *transmitter) routine() {
	var err error

	defer func() {
		p.wg.Done()

		if err != nil {
			p.onDisconnect(true, nil)
		}
	}()

	sendBuffers := net.Buffers{}
	for atomic.LoadUint32(&p.running) == 1 {
		select {
		case <-p.quit:
			err = errors.New("exit")
			atomic.StoreUint32(&p.running, 0)
			return
		case <-p.timer.C:
			if err = p.flushBuffers(sendBuffers); err != nil {
				atomic.StoreUint32(&p.running, 0)
				return
			}
			sendBuffers = net.Buffers{}

			if p.qAvailable() || p.gAvailable() {
				p.signalAvailable()
			} else {
				atomic.StoreUint32(&p.running, 0)
			}
		case <-p.available:
			// check if there any control packets except PUBLISH QoS 1/2
			// and process them
			var packets []packet.Provider
			if pkt := p.gPopPacket(); pkt != nil {
				packets = append(packets, pkt)
			}

			if pkt := p.qPopPacket(); pkt != nil {
				packets = append(packets, pkt)
			}

			prevLen := len(sendBuffers)
			for _, pkt := range packets {
				switch pkt := pkt.(type) {
				case *packet.Publish:
					p.setTopicAlias(pkt)
				}

				if sz, ok := p.packetSize(pkt); ok {
					buf := make([]byte, sz)
					if _, err = pkt.Encode(buf); err != nil {
						p.log.Error("Message encode", zap.Error(err))
					} else {
						sendBuffers = append(sendBuffers, buf)
					}
				}
			}

			available := true

			if p.qAvailable() || p.gAvailable() {
				p.signalAvailable()
			} else {
				available = false
			}

			if prevLen == 0 {
				p.timer.Reset(1 * time.Millisecond)
			} else if len(sendBuffers) >= 5 {
				p.timer.Stop()
				if err = p.flushBuffers(sendBuffers); err != nil {
					atomic.StoreUint32(&p.running, 0)
				}

				sendBuffers = net.Buffers{}

				if !available {
					atomic.StoreUint32(&p.running, 0)
				}
			}
		}
	}
}

func (p *transmitter) setTopicAlias(pkt *packet.Publish) {
	if p.topicAliasMax > 0 {
		var ok bool
		var alias uint16
		if alias, ok = p.topicAlias[pkt.Topic()]; !ok {
			if p.topicAliasCurrMax < p.topicAliasMax {
				p.topicAliasCurrMax++
				alias = p.topicAliasCurrMax
				ok = true
			} else {
				alias = uint16(rand.Intn(int(p.topicAliasMax)) + 1)
			}
		} else {
			ok = false
		}

		if err := pkt.PropertySet(packet.PropertyTopicAlias, alias); err == nil && !ok {
			pkt.SetTopic("") // nolint: errcheck
		}
	}
}
