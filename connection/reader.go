package connection

import (
	"bufio"
	"encoding/binary"
	"sync"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/transport"
	"go.uber.org/zap"
)

type reader struct {
	conn              transport.Conn
	connect           chan interface{}
	onConnectionClose signalConnectionClose
	processIncoming   signalIncoming
	log               *zap.SugaredLogger
	metric            systree.PacketsMetric
	wg                sync.WaitGroup
	connWg            sync.WaitGroup
	topicAlias        map[uint16]string
	recv              []byte
	keepAlive         time.Duration
	remaining         int
	packetMaxSize     uint32
	version           mqttp.ProtocolVersion
}

type readerOption func(*reader) error

func newReader() *reader {
	r := &reader{
		topicAlias: make(map[uint16]string),
	}

	return r
}

func (s *reader) shutdown() {
	s.wg.Wait()
	s.topicAlias = nil
	s.recv = []byte{}
	s.connect = nil
}

func (s *reader) run() {
	s.wg.Add(1)
	go s.routine()
}

func (s *reader) connection() {
	s.connWg.Wait()
	s.connWg.Add(1)
	go s.connectionRoutine()
}

func (s *reader) routine() {
	var err error

	defer func() {
		s.wg.Done()
		s.onConnectionClose(err)
	}()

	buf := bufio.NewReader(s.conn)

	for {
		var pkt mqttp.IFace

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

func (s *reader) connectionRoutine() {
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

func (s *reader) readPacket(buf *bufio.Reader) (mqttp.IFace, error) {
	var err error

	if len(s.recv) == 0 {
		var header []byte
		peekCount := 2
		// Let's read enough bytes to get the fixed header/fh (msg type/flags, remaining length)
		for {
			// max length of fh is 5 bytes
			// if we have read 5 bytes and still not done report protocol error and exit
			if peekCount > 5 {
				return nil, mqttp.CodeProtocolError
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
		// Total message length is 1 (msg type) + remLen + m (remlen bytes)
		s.remaining = 1 + int(remLen) + m
		s.recv = make([]byte, s.remaining)
	}

	if s.remaining > int(s.packetMaxSize) {
		return nil, mqttp.CodePacketTooLarge
	}

	offset := len(s.recv) - s.remaining

	for offset != s.remaining {
		var n int

		n, err = buf.Read(s.recv[offset:])
		offset += n
		if err != nil {
			s.remaining -= offset
			return nil, err
		}
	}

	var pkt mqttp.IFace
	pkt, _, err = mqttp.Decode(s.version, s.recv)

	s.recv = []byte{}
	s.remaining = 0

	return pkt, err
}
