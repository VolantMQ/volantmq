package connection

import (
	"bufio"
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/VolantMQ/volantmq/packet"
	"github.com/troian/easygo/netpoll"
)

func (s *impl) rxRun(event netpoll.Event) {
	if atomic.CompareAndSwapUint32(&s.rxRunning, 0, 1) {
		mask := netpoll.EventHup | netpoll.EventReadHup | netpoll.EventWriteHup | netpoll.EventErr | netpoll.EventPollClosed
		if (event & mask) != 0 {
			go s.onConnectionClose(nil)
		} else {
			go func() {
				s.rxWg.Wait()
				s.rxWg.Add(1)
				s.rxRoutine()
			}()
		}
	}
}

func (s *impl) rxConnection(event netpoll.Event) {
	mask := netpoll.EventHup | netpoll.EventReadHup | netpoll.EventWriteHup | netpoll.EventErr | netpoll.EventPollClosed
	if (event & mask) != 0 {
		go func() {
			s.connect <- errors.New("disconnected")
		}()
	} else {
		go func() {
			s.connectionRoutine()
		}()
	}
}

func (s *impl) rxRoutine() {
	var err error

	defer func() {
		s.rxWg.Done()
		if err != nil {
			s.onConnectionClose(err)
		}
	}()

	buf := bufio.NewReader(s.conn)

	for atomic.LoadUint32(&s.rxRunning) == 1 {
		s.runKeepAlive()

		var pkt packet.Provider
		if pkt, err = s.readPacket(buf); err == nil {
			s.metric.Received(pkt.Type())
			err = s.processIncoming(pkt)
		}

		if err != nil {
			atomic.StoreUint32(&s.rxRunning, 0)
			break
		}
	}

	if _, ok := err.(packet.ReasonCode); ok {
		return
	}

	err = s.ePoll.Resume(s.desc)
}

func (s *impl) connectionRoutine() {
	buf := bufio.NewReader(s.conn)

	pkt, err := s.readPacket(buf)

	s.keepAliveTimer.Stop()
	if err == nil {
		s.metric.Received(pkt.Type())
		err = s.processIncoming(pkt)
	}

	s.connect <- err
}

func (s *impl) readPacket(buf *bufio.Reader) (packet.Provider, error) {
	var err error

	if len(s.rxRecv) == 0 {
		var header []byte
		peekCount := 2
		// Let's read enough bytes to get the fixed header/fh (msg type/flags, remaining length)
		for {
			// max length of fh is 5 bytes
			// if we have read 5 bytes and still not done report protocol error and exit
			if peekCount > 5 {
				return nil, packet.CodeProtocolError
			}

			if header, err = buf.Peek(peekCount); err != nil {
				return nil, err
			}

			// If we got enough bytes, then check the last byte to see if the continuation
			// bit is set. If so, increment cnt and continue peeking
			if header[peekCount-1] >= 0x80 {
				peekCount++
			} else {
				break
			}
		}

		// Get the remaining length of the message
		remLen, m := binary.Uvarint(header[1:])
		// Total message length is remlen + 1 (msg type) + m (remlen bytes)
		s.rxRemaining = int(remLen) + 1 + m
		s.rxRecv = make([]byte, s.rxRemaining)
	}

	if s.rxRemaining > int(s.maxRxPacketSize) {
		return nil, packet.CodePacketTooLarge
	}

	offset := len(s.rxRecv) - s.rxRemaining

	for offset != s.rxRemaining {
		var n int

		n, err = buf.Read(s.rxRecv[offset:])
		offset += n
		if err != nil {
			s.rxRemaining -= offset
			return nil, err
		}
	}

	var pkt packet.Provider
	pkt, _, err = packet.Decode(s.version, s.rxRecv)

	s.rxRecv = []byte{}
	s.rxRemaining = 0

	return pkt, err
}
