package connection

import (
	"bufio"
	"encoding/binary"
	"sync/atomic"

	"errors"

	"github.com/VolantMQ/volantmq/packet"
	"github.com/troian/easygo/netpoll"
	"go.uber.org/zap"
)

func (s *Type) keepAliveExpired() {
	s.onConnectionClose(true, nil)
}

func (s *Type) rxRun(event netpoll.Event) {
	select {
	case <-s.quit:
		return
	default:
	}

	if atomic.CompareAndSwapUint32(&s.rxRunning, 0, 1) {
		s.rxWg.Wait()
		s.rxWg.Add(1)

		exit := false
		if event&(netpoll.EventReadHup|netpoll.EventWriteHup|netpoll.EventHup|netpoll.EventErr) != 0 {
			exit = true
		}
		if exit {
			s.log.Debug("Client connection problem, session is going to be shutdown.", zap.Any("error_reason", event.String()),
				zap.String("client_id", s.ID), zap.String("client_addr", s.Conn.RemoteAddr().String()))
			s.rxRoutine(exit)
		} else {
			s.log.Debug("Starting new receiver routine for session",
				zap.String("client_id", s.ID), zap.String("client_addr", s.Conn.RemoteAddr().String()))
			go s.rxRoutine(exit)
		}
	}
}

func (s *Type) rxRoutine(exit bool) {
	var err error

	defer func() {
		s.rxWg.Done()

		if err != nil {
			if _, ok := err.(packet.ReasonCode); !ok {
				err = nil
			}
			s.onConnectionClose(s.will, err)
		}
	}()

	if exit {
		err = errors.New("disconnect")
		return
	}

	buf := bufio.NewReader(s.Conn)

	for atomic.LoadUint32(&s.rxRunning) == 1 {
		if s.keepAlive > 0 {
			s.keepAliveTimer.Reset(s.keepAlive)
		}
		var pkt packet.Provider
		if pkt, err = s.readPacket(buf); err == nil {
			err = s.processIncoming(pkt)
		}
		if err != nil {
			atomic.StoreUint32(&s.rxRunning, 0)
		}
	}
	if _, ok := err.(packet.ReasonCode); !ok {
		s.EventPoll.Resume(s.Desc)
	}
}

func (s *Type) readPacket(buf *bufio.Reader) (packet.Provider, error) {
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

			// If not enough bytes are returned, then continue until there's enough.
			if len(header) < peekCount {
				continue
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

	if s.rxRemaining > int(s.MaxRxPacketSize) {
		return nil, packet.CodePacketTooLarge
	}

	offset := len(s.rxRecv) - s.rxRemaining

	for offset != s.rxRemaining {
		var n int
		if n, err = buf.Read(s.rxRecv[offset:]); err != nil {
			return nil, err
		}
		offset += n
	}

	var pkt packet.Provider
	pkt, _, err = packet.Decode(s.Version, s.rxRecv)

	s.rxRecv = []byte{}
	s.rxRemaining = 0

	return pkt, err
}
