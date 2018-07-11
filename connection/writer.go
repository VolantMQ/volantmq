package connection

import (
	"bufio"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/plugin/persistence"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/transport"
	"github.com/VolantMQ/volantmq/types"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type writerOption func(*writer) error

type writer struct {
	id                string
	onConnectionClose signalConnectionClose
	conn              transport.Conn
	metric            systree.PacketsMetric
	persist           persistence.Packets
	flow              flow
	pubOut            ackQueue
	gMessages         chan mqttp.IFace
	qos0Messages      chan interface{}
	qos12Messages     chan interface{}
	available         chan int
	quit              chan struct{}
	topicAlias        map[string]uint16
	wg                sync.WaitGroup
	wgRunSync         sync.WaitGroup
	wgStarted         sync.WaitGroup
	log               *zap.SugaredLogger
	onStart           sync.Once
	onStop            types.Once
	running           uint32
	pLoaderRunning    uint32
	packetMaxSize     uint32
	qos0Redirect      uint32
	qos12Redirect     uint32
	topicAliasCurrMax uint16
	topicAliasMax     uint16
	offlineQoS0       bool
	version           mqttp.ProtocolVersion
}

type sendBuffer struct {
	pktType mqttp.Type
	buffer  []byte
}

type packetLoaderCtx struct {
	unAck   bool
	count   int
	packets chan interface{}
}

func newWriter() *writer {
	w := &writer{
		gMessages:     make(chan mqttp.IFace, 0xFFFF),
		qos0Messages:  make(chan interface{}, 0xFFFF),
		qos12Messages: make(chan interface{}, 0xFFFF),
		available:     make(chan int, 1),
		topicAlias:    make(map[string]uint16),
		quit:          make(chan struct{}),
		topicAliasMax: 0,
		flow: flow{
			quota: 0xFFFF,
		},
		qos0Redirect:  1,
		qos12Redirect: 1,
		running:       0,
		packetMaxSize: 0xFFFFFFF,
	}

	w.wgStarted.Add(1)

	w.pubOut.onRelease = w.onReleaseOut

	return w
}

func (s *writer) isAlive() bool {
	select {
	case <-s.quit:
		return false
	default:
	}

	return true
}

func (s *writer) start(start bool) {
	s.onStart.Do(func() {
		defer func() {
			s.wgStarted.Done()
		}()

		if start {
			ctx := &packetLoaderCtx{
				unAck:   true,
				count:   cap(s.qos12Messages),
				packets: s.qos12Messages,
			}

			s.persist.PacketsForEachUnAck([]byte(s.id), ctx, s.packetLoader)

			ctx.unAck = false

			ctx.count = cap(s.qos12Messages) - len(s.qos12Messages)

			if ctx.count > 0 {
				s.persist.PacketsForEachQoS12([]byte(s.id), ctx, s.packetLoader)
			}

			ctx.packets = s.qos0Messages
			ctx.count = cap(s.qos0Messages) - len(s.qos0Messages)
			s.persist.PacketsForEachQoS0([]byte(s.id), ctx, s.packetLoader)

			s.signalAndRun()
		}
	})
}

func (s *writer) packetLoader(c interface{}, entry *persistence.PersistedPacket) (bool, error) {
	ctx := c.(*packetLoaderCtx)

	var e error
	var pkt mqttp.IFace

	if pkt, _, e = mqttp.Decode(s.version, entry.Data); e != nil {
		s.log.Error("decode persisted message", zap.String("clientId", s.id), zap.Error(e))
		return true, nil
	}

	if ctx.unAck {
		switch p := pkt.(type) {
		case *mqttp.Publish:
			id, _ := p.ID()
			s.flow.reAcquire(id)
		case *mqttp.Ack:
			id, _ := p.ID()
			s.flow.reAcquire(id)
		}
	} else if p, ok := pkt.(*mqttp.Publish); ok {
		if len(entry.ExpireAt) > 0 {
			var tm time.Time
			if tm, e = time.Parse(time.RFC3339, entry.ExpireAt); e == nil {
				p.SetExpireAt(tm)
			} else {
				s.log.Error("Parse publish expiry", zap.String("ClientID", s.id), zap.Error(e))
			}
		}
	}

	ctx.packets <- pkt

	ctx.count--

	if ctx.count == 0 {
		return true, errors.New("exit")
	}

	return true, nil
}

func (s *writer) stop() {
	s.onStop.Do(func() {
		s.wgStarted.Wait()
		close(s.quit)
		atomic.StoreUint32(&s.running, 0)

		s.wg.Wait()

		atomic.StoreUint32(&s.qos0Redirect, 1)
		atomic.StoreUint32(&s.qos12Redirect, 1)

		close(s.available)
		select {
		case <-s.available:
		default:
		}
	})
}

func (s *writer) shutdown() {
	close(s.gMessages)
	close(s.qos0Messages)
	close(s.qos12Messages)

	s.topicAlias = nil
}

func (s *writer) send(pkt mqttp.IFace) {
	if ok := s.packetFitsSize(pkt); !ok {
		return
	}

	switch pkt.Type() {
	case mqttp.PUBLISH:
		p := pkt.(*mqttp.Publish)
		if p.QoS() == mqttp.QoS0 {
			s.sendQoS0(pkt)
		} else {
			s.sendQoS12(pkt)
		}
	default:
		s.sendGeneric(pkt)
	}
}

func (s *writer) signalAndRun() {
	s.signalAvailable()
	s.run()
}

func (s *writer) sendGeneric(pkt mqttp.IFace) {
	s.gMessages <- pkt
	s.signalAndRun()
}

func (s *writer) sendQoS0(pkt mqttp.IFace) {
	if (atomic.LoadUint32(&s.qos0Redirect) == 0) && ((cap(s.qos0Messages) - len(s.qos0Messages)) > 0) {
		s.qos0Messages <- pkt
	} else {
		atomic.StoreUint32(&s.qos0Redirect, 1)
		if p := s.encodeForPersistence(pkt); p != nil {
			if err := s.persist.PacketStoreQoS0([]byte(s.id), p); err != nil {
				s.log.Error("persist packet", zap.String("clientId", s.id), zap.Error(err))
			}
		}
	}
	s.signalAndRun()
}

func (s *writer) sendQoS12(pkt mqttp.IFace) {
	if (atomic.LoadUint32(&s.qos12Redirect) == 0) && ((cap(s.qos12Messages) - len(s.qos12Messages)) > 0) {
		s.qos12Messages <- pkt
	} else {
		atomic.StoreUint32(&s.qos12Redirect, 1)
		if p := s.encodeForPersistence(pkt); p != nil {
			if err := s.persist.PacketStoreQoS12([]byte(s.id), p); err != nil {
				s.log.Error("persist packet", zap.String("clientId", s.id), zap.Error(err))
			}
		}
	}

	s.signalAndRun()
}

func (s *writer) signalAvailable() {
	if !s.isAlive() {
		return
	}

	select {
	case s.available <- 1:
	default:
	}
}

func (s *writer) signalTxQuotaAvailable() {
	if len(s.qos12Messages) > 0 {
		s.signalAndRun()
	}
}

func (s *writer) run() {
	if !s.isAlive() {
		return
	}

	if atomic.CompareAndSwapUint32(&s.running, 0, 1) {
		s.wg.Add(1)
		go s.routine()
	}
}

func (s *writer) gAvailable() bool {
	return len(s.gMessages) > 0
}

func (s *writer) qos0Available() bool {
	return len(s.gMessages) > 0
}

func (s *writer) qos12Available() bool {
	return s.flow.quotaAvailable() && (len(s.qos12Messages) > 0)
}

func (s *writer) gPopPacket() mqttp.IFace {
	select {
	case p := <-s.gMessages:
		return p
	default:
		return nil
	}
}

func (s *writer) qos0PopPacket() mqttp.IFace {
	select {
	case p := <-s.qos0Messages:
		return p.(mqttp.IFace)
	default:
		return nil
	}
}

func (s *writer) qos12PopPacket() mqttp.IFace {
	var pkt mqttp.IFace

	if s.flow.quotaAvailable() && len(s.qos12Messages) > 0 {
		value := <-s.qos12Messages
		switch m := value.(type) {
		case *mqttp.Publish:
			// try acquire packet id
			id, _ := s.flow.acquire()

			m.SetPacketID(id)
			pkt = m
		case *unacknowledged:
			pkt = m
		default:
			s.log.Panic("unexpected type")
		}

		s.pubOut.store(pkt)
	}

	return pkt
}

func (s *writer) routine() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
		}
	}()

	var err error

	// waiting previous routine finished
	s.wgRunSync.Wait()

	if !s.isAlive() {
		s.wg.Done()
		return
	}

	s.wgRunSync.Add(1)

	wr := bufio.NewWriter(s.conn)

	defer func() {
		atomic.StoreUint32(&s.running, 0)

		if err == nil && s.isAlive() {
			wr.Flush()
		}

		if err == nil {
			if len(s.available) > 0 {
				s.run()
			}
		}

		s.wgRunSync.Done()
		s.wg.Done()

		if err != nil {
			s.onConnectionClose(err)
		}
	}()

	for atomic.LoadUint32(&s.running) == 1 {
		if !s.isAlive() {
			return
		}

		select {
		case <-s.available:
			var sendBuffers []sendBuffer

			if packets := s.popPackets(); len(packets) > 0 {
				for _, pkt := range packets {
					switch pack := pkt.(type) {
					case *mqttp.Publish:
						if _, expireLeft, expired := pack.Expired(); expired {
							continue
						} else {
							if expireLeft > 0 {
								if err = pkt.PropertySet(mqttp.PropertyPublicationExpiry, expireLeft); err != nil {
									s.log.Error("Set publication expire", zap.String("ClientID", s.id), zap.Error(err))
								}
							}
							s.setTopicAlias(pack)
						}
					}

					if buf, e := mqttp.Encode(pkt); e != nil {
						s.log.Error("packet encode", zap.String("ClientID", s.id), zap.Error(err))
					} else {
						sendBuffers = append(sendBuffers, sendBuffer{pktType: pkt.Type(), buffer: buf})
					}
				}
			}

			for _, b := range sendBuffers {
				if _, err = wr.Write(b.buffer); err != nil {
					return
				} else {
					s.metric.Sent(b.pktType)
				}
			}

			if s.qos12Available() || s.qos0Available() || s.gAvailable() {
				s.signalAvailable()
			}
		default:
			// when no messages to transmit see if persistence has anything
			// running it as separate goroutine allows to gracefully handle
			// SUBSCRIBE/UNSUBSCRIBE/PING message if load from persistence takes a while

			atomic.StoreUint32(&s.qos0Redirect, 1)
			atomic.StoreUint32(&s.qos12Redirect, 1)

			if atomic.LoadUint32(&s.pLoaderRunning) == 0 {
				s.wg.Add(1)
				atomic.StoreUint32(&s.pLoaderRunning, 1)
				go s.loadFromPersistence()
			}

			return
		}
	}
}

func (s *writer) popPackets() []mqttp.IFace {
	var packets []mqttp.IFace
	if s.isAlive() {
		if pkt := s.gPopPacket(); pkt != nil {
			packets = append(packets, pkt)
		}

		if pkt := s.qos12PopPacket(); pkt != nil {
			packets = append(packets, pkt)
		}

		if pkt := s.qos0PopPacket(); pkt != nil {
			packets = append(packets, pkt)
		}
	}
	return packets
}

func (s *writer) setTopicAlias(pkt *mqttp.Publish) {
	if s.topicAliasMax > 0 {
		var exists bool
		var alias uint16
		if alias, exists = s.topicAlias[pkt.Topic()]; !exists {
			if s.topicAliasCurrMax < s.topicAliasMax {
				s.topicAliasCurrMax++
				alias = s.topicAliasCurrMax
			} else {
				alias = uint16(rand.Intn(int(s.topicAliasMax)))
			}

			s.topicAlias[pkt.Topic()] = alias
		}

		if err := pkt.PropertySet(mqttp.PropertyTopicAlias, alias); err == nil && exists {
			pkt.SetTopic("") // nolint: errcheck
		}
	}
}

func (s *writer) loadFromPersistence() {
	var err error
	defer func() {
		atomic.StoreUint32(&s.pLoaderRunning, 0)
		s.wg.Done()

		if r := recover(); r != nil {
			s.onConnectionClose(errors.New("close on panic"))
		}
		if err != nil {
			s.onConnectionClose(err)
		}
	}()

	signal := false

	toLoad := cap(s.qos0Messages) - len(s.qos0Messages)
	if toLoad > 0 {
		if cnt, _ := s.persist.PacketCountQoS0([]byte(s.id)); cnt > 0 {
			signal = true

			ctx := &packetLoaderCtx{
				count:   toLoad,
				packets: s.qos0Messages,
			}

			s.persist.PacketsForEachQoS0([]byte(s.id), ctx, s.packetLoader)

			if cnt, _ = s.persist.PacketCountQoS0([]byte(s.id)); cnt == 0 {
				atomic.StoreUint32(&s.qos0Redirect, 0)
			}
		} else {
			atomic.StoreUint32(&s.qos0Redirect, 0)
		}
	}

	toLoad = cap(s.qos12Messages) - len(s.qos12Messages)
	if toLoad > 0 {
		if cnt, _ := s.persist.PacketCountQoS12([]byte(s.id)); cnt > 0 {
			signal = true

			ctx := &packetLoaderCtx{
				count:   toLoad,
				packets: s.qos12Messages,
			}

			s.persist.PacketsForEachQoS12([]byte(s.id), ctx, s.packetLoader)

			if cnt, _ = s.persist.PacketCountQoS12([]byte(s.id)); cnt == 0 {
				atomic.StoreUint32(&s.qos12Redirect, 0)
			}
		} else {
			atomic.StoreUint32(&s.qos12Redirect, 0)
		}
	}

	if signal {
		s.signalAndRun()
	}
}

func (s *writer) packetFitsSize(value interface{}) bool {
	var sz int
	var err error
	if obj, ok := value.(sizeAble); !ok {
		s.log.Fatal("Object does not belong to allowed types",
			zap.String("ClientID", s.id),
			zap.String("Type", reflect.TypeOf(value).String()))
	} else {
		if sz, err = obj.Size(); err != nil {
			s.log.Error("Couldn't calculate message size", zap.String("ClientID", s.id), zap.Error(err))
			return false
		}
	}

	// ignore any packet with size bigger than negotiated
	if sz > int(s.packetMaxSize) {
		s.log.Warn("Ignore packet with size bigger than negotiated with client",
			zap.String("ClientID", s.id),
			zap.Uint32("negotiated", s.packetMaxSize),
			zap.Int("actual", sz))
		return false
	}

	return true
}

func (s *writer) releaseID(id mqttp.IDType) {
	if s.flow.release(id) {
		s.signalTxQuotaAvailable()
	}
}

// onReleaseOut process messages that required ack cycle
// onAckTimeout if publish message has not been acknowledged withing specified ackTimeout
// server should mark it as a dup and send again
func (s *writer) onReleaseOut(o, n mqttp.IFace) {
	switch n.Type() {
	case mqttp.PUBACK:
		fallthrough
	case mqttp.PUBCOMP:
		id, _ := n.ID()
		s.releaseID(id)
	}
}

func (s *writer) encodeForPersistence(pkt mqttp.IFace) *persistence.PersistedPacket {
	pPkt := &persistence.PersistedPacket{}

	switch tp := pkt.(type) {
	case *mqttp.Publish:
		if expireAt, _, expired := tp.Expired(); !expired {
			if !expireAt.IsZero() {
				pPkt.ExpireAt = expireAt.Format(time.RFC3339)
			}

			if tp.QoS() != mqttp.QoS0 {
				// make sure message has some IDType to prevent encode error
				tp.SetPacketID(0)
			}

			pkt = tp
		} else {
			pkt = nil
		}
	}

	if pkt != nil {
		var err error
		if pPkt.Data, err = mqttp.Encode(pkt); err != nil {
			s.log.Error("Couldn't encode message for persistence", zap.Error(err))
		} else {
			return pPkt
		}
	}

	return nil
}

func (s *writer) getQueuedPackets() persistence.PersistedPackets {
	var packets persistence.PersistedPackets

	packetEncode := func(p interface{}) *persistence.PersistedPacket {
		var pkt mqttp.IFace
		pPkt := &persistence.PersistedPacket{}

		switch tp := p.(type) {
		case *mqttp.Publish:
			if expireAt, _, expired := tp.Expired(); !expired {
				if !expireAt.IsZero() {
					pPkt.ExpireAt = expireAt.Format(time.RFC3339)
				}

				if tp.QoS() != mqttp.QoS0 {
					// make sure message has some IDType to prevent encode error
					tp.SetPacketID(0)
				}

				pkt = tp
			}
		case *unacknowledged:
			if pb, ok := p.(*mqttp.Publish); ok && pb.QoS() == mqttp.QoS1 {
				pb.SetDup(true)
			}

			pkt = tp
		default:
			s.log.Error("invalid type")
		}

		if pkt != nil {
			var err error
			if pPkt.Data, err = mqttp.Encode(pkt); err != nil {
				s.log.Error("Couldn't encode message for persistence", zap.Error(err))
			} else {
				return pPkt
			}
		}

		return nil
	}

	for m := range s.qos0Messages {
		if s.offlineQoS0 {
			packets.QoS0 = append(packets.QoS0, packetEncode(m))
		}
	}

	for m := range s.qos12Messages {
		packets.QoS12 = append(packets.QoS12, packetEncode(m))
	}

	s.pubOut.messages.Range(func(k, v interface{}) bool {
		if pkt, ok := v.(mqttp.IFace); ok {
			packets.UnAck = append(packets.UnAck, packetEncode(&unacknowledged{pkt}))
		}

		s.pubOut.messages.Delete(k)
		return true
	})

	return packets
}
