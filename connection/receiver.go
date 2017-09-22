package connection

import (
	"bufio"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VolantMQ/volantmq/packet"
	"github.com/troian/easygo/netpoll"
)

type receiverConfig struct {
	conn           net.Conn
	keepAliveTimer *time.Timer // nolint: structcheck
	will           *bool
	quit           chan struct{}
	keepAlive      time.Duration // nolint: structcheck
	waitForRead    func() error
	onPacket       func(packet.Provider) error
	onDisconnect   func(bool, error) // nolint: megacheck
	maxPacketSize  uint32
	version        packet.ProtocolVersion
}

type receiver struct {
	receiverConfig
	wg            sync.WaitGroup
	recv          []byte
	remainingRecv int
	running       uint32
}

func newReceiver(config *receiverConfig) *receiver {
	r := &receiver{
		receiverConfig: *config,
	}

	if r.keepAlive > 0 {
		r.keepAliveTimer = time.AfterFunc(r.keepAlive, r.keepAliveExpired)
	}

	return r
}

func (r *receiver) keepAliveExpired() {
	r.onDisconnect(true, nil)
}

func (r *receiver) shutdown() {
	r.wg.Wait()

	if r.keepAlive > 0 {
		r.keepAliveTimer.Stop()
		r.keepAliveTimer = nil
	}
}

func (r *receiver) run(event netpoll.Event) {
	select {
	case <-r.quit:
		return
	default:
	}

	if atomic.CompareAndSwapUint32(&r.running, 0, 1) {
		r.wg.Wait()
		r.wg.Add(1)

		exit := false
		if event&(netpoll.EventReadHup|netpoll.EventWriteHup|netpoll.EventHup|netpoll.EventErr) != 0 {
			exit = true
		}

		go r.routine(exit)
	}
}

func (r *receiver) routine(exit bool) {
	var err error

	signalDisconnect := func() {
		r.wg.Done()
		if _, ok := err.(packet.ReasonCode); !ok {
			err = nil
		}
		r.onDisconnect(*r.will, err)
	}

	if exit {
		defer signalDisconnect()
		return
	}

	buf := bufio.NewReader(r.conn)

	for atomic.LoadUint32(&r.running) == 1 {
		if r.keepAlive > 0 {
			r.keepAliveTimer.Reset(r.keepAlive)
		}
		var pkt packet.Provider
		if pkt, err = r.readPacket(buf); err == nil {
			err = r.onPacket(pkt)
		}

		if err != nil {
			atomic.StoreUint32(&r.running, 0)
		}
	}

	if _, ok := err.(packet.ReasonCode); ok {
		defer signalDisconnect()
	} else {
		r.wg.Done()
		if err = r.waitForRead(); err != nil {
			defer signalDisconnect()
		}
	}
}

func (r *receiver) readPacket(buf *bufio.Reader) (packet.Provider, error) {
	var err error

	if len(r.recv) == 0 {
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
		r.remainingRecv = int(remLen) + 1 + m
		r.recv = make([]byte, r.remainingRecv)
	}

	if r.remainingRecv > int(r.maxPacketSize) {
		return nil, packet.CodePacketTooLarge
	}

	offset := len(r.recv) - r.remainingRecv

	for offset != r.remainingRecv {
		var n int
		if n, err = buf.Read(r.recv[offset:]); err != nil {
			return nil, err
		}
		offset += n
	}

	var pkt packet.Provider
	pkt, _, err = packet.Decode(r.version, r.recv)

	r.recv = []byte{}
	r.remainingRecv = 0

	return pkt, err
}
