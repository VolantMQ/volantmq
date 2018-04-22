package connection

import (
	"bufio"
	"encoding/binary"
	"time"

	"github.com/VolantMQ/mqttp"
)

func (s *impl) rxShutdown() {
	s.rxWg.Wait()
}

func (s *impl) rxRun() {
	s.rxWg.Add(1)
	go s.rxRoutine()
}

//func (s *impl) rxRun(events netpoll.Event) {
//	mask := netpoll.EventHup | netpoll.EventReadHup | netpoll.EventWriteHup | netpoll.EventErr | netpoll.EventPollClosed
//	if (events & mask) != 0 {
//		go s.onConnectionClose(nil)
//	} else {
//		s.rxWg.Wait()
//		s.rxWg.Add(1)
//		go s.rxRoutine()
//	}
//}

func (s *impl) rxConnection() {
	s.connWg.Wait()
	s.connWg.Add(1)
	go s.connectionRoutine()
}

//func (s *impl) rxConnection(events netpoll.Event) {
//	mask := netpoll.EventHup | netpoll.EventReadHup | netpoll.EventWriteHup | netpoll.EventErr | netpoll.EventPollClosed
//	if (events & mask) != 0 {
//		go func() {
//			s.connect <- errors.New("disconnected")
//		}()
//	} else {
//		s.connWg.Wait()
//		s.connWg.Add(1)
//		go s.connectionRoutine()
//	}
//}

func (s *impl) rxRoutine() {
	var err error

	defer func() {
		s.rxWg.Done()
		s.onConnectionClose(err)
	}()

	buf := bufio.NewReader(s.conn)

	for {
		var pkt packet.Provider

		if s.keepAlive.Nanoseconds() > 0 {
			if err = s.conn.SetReadDeadline(time.Now().Add(s.keepAlive)); err != nil {
				return
			}
		}

		if pkt, err = s.readPacket(buf); err != nil {
			return
		}

		s.metric.Received(pkt.Type())
		if err = s.processIncoming(pkt); err != nil {
			return
		}
	}
}

//func (s *impl) rxRoutine() {
//	var err error
//
//	defer func() {
//		s.rxWg.Done()
//		if err != nil {
//			s.onConnectionClose(err)
//		}
//	}()
//
//	buf := bufio.NewReader(s.conn)
//
//	var pkt packet.Provider
//
//	if pkt, err = s.readPacket(buf); err != nil {
//		return
//	}
//
//	s.metric.Received(pkt.Type())
//	if err = s.processIncoming(pkt); err != nil {
//		return
//	}
//
//	if s.keepAlive > 0 {
//		if err = s.conn.SetReadDeadline(time.Now().Add(s.keepAlive)); err != nil {
//			return
//		}
//	}
//
//	err = s.conn.Resume()
//}

func (s *impl) connectionRoutine() {
	defer s.connWg.Done()

	if s.keepAlive.Nanoseconds() > 0 {
		if err := s.conn.SetReadDeadline(time.Now().Add(s.keepAlive)); err != nil {
			s.connect <- err
			return
		}
	}

	buf := bufio.NewReader(s.conn)

	if pkt, err := s.readPacket(buf); err == nil {
		s.metric.Received(pkt.Type())
		err = s.processIncoming(pkt)
	} else {
		s.connect <- err
	}
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
