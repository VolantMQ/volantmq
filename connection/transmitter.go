package connection

import (
	"container/list"
	"math/rand"
	"net"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/VolantMQ/mqttp"
	"go.uber.org/zap"
)

func (s *impl) gPushFront(pkt packet.Provider) {
	s.txGLock.Lock()
	s.txGMessages.PushFront(pkt)
	s.txGLock.Unlock()
	s.txSignalAvailable()
	s.txRun()
}

func (s *impl) gPush(pkt packet.Provider) {
	s.txGLock.Lock()
	s.txGMessages.PushBack(pkt)
	s.txGLock.Unlock()
	s.txSignalAvailable()
	s.txRun()
}

func (s *impl) gLoad(pkt packet.Provider) {
	s.txGLock.Lock()
	s.txGMessages.PushBack(pkt)
	s.txGLock.Unlock()
	s.txSignalAvailable()
}

func (s *impl) gLoadList(l *list.List) {
	s.txGLock.Lock()
	s.txGMessages.PushBackList(l)
	s.txGLock.Unlock()
}

func (s *impl) qPush(pkt interface{}) {
	s.txQLock.Lock()
	s.txQMessages.PushBack(pkt)
	s.txQLock.Unlock()
	s.txSignalAvailable()
	s.txRun()
}

func (s *impl) qLoad(pkt interface{}) {
	s.txQLock.Lock()
	s.txQMessages.PushBack(pkt)
	s.txQLock.Unlock()
	s.txSignalAvailable()
}

func (s *impl) qLoadList(l *list.List) {
	s.txQLock.Lock()
	s.txQMessages.PushBackList(l)
	s.txQLock.Unlock()
}

func (s *impl) txSignalAvailable() {
	select {
	case <-s.quit:
		return
	default:
		select {
		case s.txAvailable <- 1:
		default:
		}
	}
}

func (s *impl) signalQuota() {
	s.txQLock.Lock()
	s.txQuotaExceeded = false
	l := s.txQMessages.Len()
	s.txQLock.Unlock()

	if l > 0 {
		s.txSignalAvailable()
		s.txRun()
	}
}

func (s *impl) txRun() {
	select {
	case <-s.quit:
		return
	default:
	}

	if atomic.CompareAndSwapUint32(&s.txRunning, 0, 1) {
		s.txWg.Wait()
		s.txWg.Add(1)
		go s.txRoutine()
	}
}

func (s *impl) flushBuffers(buf net.Buffers) error {
	_, e := buf.WriteTo(s.conn)
	buf = net.Buffers{}
	return e
}

func (s *impl) packetFitsSize(value interface{}) bool {
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
	if sz > int(s.maxTxPacketSize) {
		s.log.Warn("Ignore packet with size bigger than negotiated with client",
			zap.String("ClientID", s.id),
			zap.Uint32("negotiated", s.maxTxPacketSize),
			zap.Int("actual", sz))
		return false
	}

	return true
}

func (s *impl) gAvailable() bool {
	defer s.txGLock.Unlock()
	s.txGLock.Lock()

	return s.txGMessages.Len() > 0
}

func (s *impl) qAvailable() bool {
	defer s.txQLock.Unlock()
	s.txQLock.Lock()

	return !s.txQuotaExceeded && s.txQMessages.Len() > 0
}

func (s *impl) gPopPacket() packet.Provider {
	defer s.txGLock.Unlock()
	s.txGLock.Lock()

	var elem *list.Element

	if elem = s.txGMessages.Front(); elem == nil {
		return nil
	}

	return s.txGMessages.Remove(elem).(packet.Provider)
}

func (s *impl) qPopPacket() packet.Provider {
	defer s.txQLock.Unlock()
	s.txQLock.Lock()

	var pkt packet.Provider

	if elem := s.txQMessages.Front(); !s.txQuotaExceeded && elem != nil {
		value := elem.Value
		switch m := value.(type) {
		case *packet.Publish:
			// try acquire packet id
			id, err := s.flowAcquire()
			if err == errExit {
				atomic.StoreUint32(&s.txRunning, 0)
				return nil
			} else if err == errQuotaExceeded {
				s.txQuotaExceeded = true
			}

			m.SetPacketID(id)
			pkt = m
		case *unacknowledged:
			pkt = m.packet

		}
		s.txQMessages.Remove(elem)
		s.pubOut.store(pkt)
	}

	return pkt
}

func (s *impl) txRoutine() {
	var err error

	defer func() {
		s.txWg.Done()

		if err != nil {
			s.onConnectionClose(err)
		}
	}()

	sendBuffers := net.Buffers{}

	for atomic.LoadUint32(&s.txRunning) == 1 {
		select {
		case <-s.txAvailable:
			prevLen := len(sendBuffers)

			for packets := s.popPackets(); len(packets) > 0; packets = s.popPackets() {
				for _, pkt := range packets {
					switch pack := pkt.(type) {
					case *packet.Publish:
						if _, expireLeft, expired := pack.Expired(); expired {
							continue
						} else {
							if expireLeft > 0 {
								if err = pkt.PropertySet(packet.PropertyPublicationExpiry, expireLeft); err != nil {
									s.log.Error("Set publication expire", zap.String("ClientID", s.id), zap.Error(err))
								}
							}
							s.setTopicAlias(pack)
						}
					}

					if ok := s.packetFitsSize(pkt); ok {
						if buf, e := packet.Encode(pkt); e != nil {
							s.log.Error("Message encode", zap.String("ClientID", s.id), zap.Error(err))
						} else {
							// todo (troian) might not be good place to do metrics
							s.metric.Sent(pkt.Type())
							sendBuffers = append(sendBuffers, buf)
						}
					}
				}

				if len(sendBuffers) >= 5 {
					break
				}
			}

			available := false
			if s.qAvailable() || s.gAvailable() {
				s.txSignalAvailable()
				available = true
			}

			if prevLen == 0 && len(sendBuffers) < 5 {
				s.txTimer.Reset(1 * time.Millisecond)
			} else if len(sendBuffers) >= 5 {
				s.txTimer.Stop()
				if err = s.flushBuffers(sendBuffers); err != nil {
					atomic.StoreUint32(&s.txRunning, 0)
					return
				}

				sendBuffers = net.Buffers{}

				if !available {
					atomic.StoreUint32(&s.txRunning, 0)
				}
			}
		case <-s.txTimer.C:
			if err = s.flushBuffers(sendBuffers); err != nil {
				atomic.StoreUint32(&s.txRunning, 0)
				return
			}

			sendBuffers = net.Buffers{}

			if s.qAvailable() || s.gAvailable() {
				s.txSignalAvailable()
			} else {
				atomic.StoreUint32(&s.txRunning, 0)
			}
		case <-s.quit:
			atomic.StoreUint32(&s.txRunning, 0)
			return
		}
	}
}

// +
func (s *impl) popPackets() []packet.Provider {
	var packets []packet.Provider
	if pkt := s.gPopPacket(); pkt != nil {
		packets = append(packets, pkt)
	}

	if pkt := s.qPopPacket(); pkt != nil {
		packets = append(packets, pkt)
	}

	return packets
}

// +
func (s *impl) setTopicAlias(pkt *packet.Publish) {
	if s.maxTxTopicAlias > 0 {
		var exists bool
		var alias uint16
		if alias, exists = s.txTopicAlias[pkt.Topic()]; !exists {
			if s.topicAliasCurrMax < s.maxTxTopicAlias {
				s.topicAliasCurrMax++
				alias = s.topicAliasCurrMax
			} else {
				alias = uint16(rand.Intn(int(s.maxTxTopicAlias)))
			}

			s.txTopicAlias[pkt.Topic()] = alias
		}

		if err := pkt.PropertySet(packet.PropertyTopicAlias, alias); err == nil && exists {
			pkt.SetTopic("") // nolint: errcheck
		}
	}
}
