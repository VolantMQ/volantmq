package connection

import (
	"container/list"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"reflect"

	"math/rand"

	"github.com/VolantMQ/volantmq/packet"
	"go.uber.org/zap"
)

func (s *Type) gPush(pkt packet.Provider) {
	s.txGLock.Lock()
	s.txGMessages.PushBack(pkt)
	s.txGLock.Unlock()
	s.txSignalAvailable()
	s.txRun()
}

func (s *Type) gLoad(pkt packet.Provider) {
	s.txGMessages.PushBack(pkt)
	s.txSignalAvailable()
}

func (s *Type) gLoadList(l *list.List) {
	s.txGMessages.PushBackList(l)
	s.txSignalAvailable()
}

func (s *Type) qPush(pkt interface{}) {
	s.txQLock.Lock()
	s.txQMessages.PushBack(pkt)
	s.txQLock.Unlock()
	s.txSignalAvailable()
	s.txRun()
}

func (s *Type) qLoad(pkt interface{}) {
	s.txQMessages.PushBack(pkt)
	s.txSignalAvailable()
}

func (s *Type) qLoadList(l *list.List) {
	s.txQMessages.PushBackList(l)
	s.txSignalAvailable()
}

func (s *Type) txSignalAvailable() {
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

func (s *Type) signalQuota() {
	s.txQLock.Lock()
	s.txQuotaExceeded = false
	l := s.txQMessages.Len()
	s.txQLock.Unlock()

	if l > 0 {
		s.txSignalAvailable()
		s.txRun()
	}
}

func (s *Type) txRun() {
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

func (s *Type) flushBuffers(buf net.Buffers) error {
	_, e := buf.WriteTo(s.Conn)
	buf = net.Buffers{}
	// todo metrics
	return e
}

func (s *Type) packetFitsSize(value interface{}) bool {
	var sz int
	var err error
	if obj, ok := value.(sizeAble); !ok {
		s.log.Fatal("Object does not belong to allowed types",
			zap.String("ClientID", s.ID),
			zap.String("Type", reflect.TypeOf(value).String()))
	} else {
		if sz, err = obj.Size(); err != nil {
			s.log.Error("Couldn't calculate message size", zap.String("ClientID", s.ID), zap.Error(err))
			return false
		}
	}

	// ignore any packet with size bigger than negotiated
	if sz > int(s.MaxTxPacketSize) {
		s.log.Warn("Ignore packet with size bigger than negotiated with client",
			zap.String("ClientID", s.ID),
			zap.Uint32("negotiated", s.MaxTxPacketSize),
			zap.Int("actual", sz))
		return false
	}

	return true
}

func (s *Type) gAvailable() bool {
	defer s.txGLock.Unlock()
	s.txGLock.Lock()

	return s.txGMessages.Len() > 0
}

func (s *Type) qAvailable() bool {
	defer s.txQLock.Unlock()
	s.txQLock.Lock()

	return !s.txQuotaExceeded && s.txQMessages.Len() > 0
}

func (s *Type) gPopPacket() packet.Provider {
	defer s.txGLock.Unlock()
	s.txGLock.Lock()

	var elem *list.Element

	if elem = s.txGMessages.Front(); elem == nil {
		return nil
	}

	value := s.txGMessages.Remove(elem)

	return value.(packet.Provider)
}

func (s *Type) qPopPacket() packet.Provider {
	defer s.txQLock.Unlock()
	s.txQLock.Lock()

	if elem := s.txQMessages.Front(); !s.txQuotaExceeded && elem != nil {
		var pkt packet.Provider
		value := elem.Value
		switch m := value.(type) {
		case *packet.Publish:
			// try acquire packet id
			id, err := s.flowAcquire()
			if err == errExit {
				atomic.StoreUint32(&s.txRunning, 0)
				return nil
			}

			if err == errQuotaExceeded {
				s.txQuotaExceeded = true
			}

			m.SetPacketID(id)
			pkt = m
		case *unacknowledged:
			pkt = m.packet

		}
		s.txQMessages.Remove(elem)
		s.pubOut.store(pkt)

		return pkt
	}

	return nil
}

func (s *Type) txRoutine() {
	var err error

	defer func() {
		s.txWg.Done()

		if err != nil {
			s.onConnectionClose(true, nil)
		}
	}()

	sendBuffers := net.Buffers{}
	for atomic.LoadUint32(&s.txRunning) == 1 {
		select {
		case <-s.quit:
			err = errors.New("exit")
			atomic.StoreUint32(&s.txRunning, 0)
			return
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
		case <-s.txAvailable:
			// check if there any control packets except PUBLISH QoS 1/2
			// and process them
			prevLen := len(sendBuffers)
			for _, pkt := range s.popPackets() {
				switch _p := pkt.(type) {
				case *packet.Publish:
					if _p.Expired(true) {
						pkt = nil
					} else {
						s.setTopicAlias(_p)
					}
				}

				if pkt != nil {
					if ok := s.packetFitsSize(pkt); ok {
						if buf, e := packet.Encode(pkt); e != nil {
							s.log.Error("Message encode", zap.String("ClientID", s.ID), zap.Error(err))
						} else {
							sendBuffers = append(sendBuffers, buf)
						}
					}
				}
			}

			available := true

			if s.qAvailable() || s.gAvailable() {
				s.txSignalAvailable()
			} else {
				available = false
			}

			if prevLen == 0 {
				s.txTimer.Reset(1 * time.Millisecond)
			} else if len(sendBuffers) >= 5 {
				s.txTimer.Stop()
				if err = s.flushBuffers(sendBuffers); err != nil {
					atomic.StoreUint32(&s.txRunning, 0)
				}

				sendBuffers = net.Buffers{}

				if !available {
					atomic.StoreUint32(&s.txRunning, 0)
				}
			}
		}
	}
}

func (s *Type) popPackets() []packet.Provider {
	var packets []packet.Provider
	if pkt := s.gPopPacket(); pkt != nil {
		packets = append(packets, pkt)
	}

	if pkt := s.qPopPacket(); pkt != nil {
		packets = append(packets, pkt)
	}

	return packets
}

func (s *Type) setTopicAlias(pkt *packet.Publish) {
	if s.MaxTxTopicAlias > 0 {
		var ok bool
		var alias uint16
		if alias, ok = s.txTopicAlias[pkt.Topic()]; !ok {
			if s.topicAliasCurrMax < s.MaxTxTopicAlias {
				s.topicAliasCurrMax++
				alias = s.topicAliasCurrMax
				ok = true
			} else {
				alias = uint16(rand.Intn(int(s.MaxTxTopicAlias)) + 1)
			}
		} else {
			ok = false
		}

		if err := pkt.PropertySet(packet.PropertyTopicAlias, alias); err == nil && !ok {
			pkt.SetTopic("") // nolint: errcheck
		}
	}
}
