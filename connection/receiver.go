package connection

import (
	"bufio"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VolantMQ/volantmq/packet"
)

type receiverConfig struct {
	conn         net.Conn
	quit         chan struct{}
	keepAlive    time.Duration // nolint: structcheck
	onPacket     func(packet.Provider) error
	onDisconnect func(bool)
	will         *bool
	version      packet.ProtocolVersion
}

type receiver struct {
	receiverConfig
	wg            sync.WaitGroup
	running       uint32
	recv          []byte
	remainingRecv int
}

func newReceiver(config *receiverConfig) *receiver {
	r := &receiver{
		receiverConfig: *config,
	}
	return r
}

func (r *receiver) shutdown() {
	r.wg.Wait()
}

func (r *receiver) run() {
	if atomic.CompareAndSwapUint32(&r.running, 0, 1) {
		r.wg.Wait()
		r.wg.Add(1)
		if r.keepAlive > 0 {
			r.conn.SetReadDeadline(time.Now().Add(r.keepAlive)) // nolint: errcheck
		} else {
			r.conn.SetReadDeadline(time.Time{}) // nolint: errcheck
		}

		go r.routine()
	}
}

func (r *receiver) routine() {
	var err error
	defer func() {
		r.wg.Done()
		r.onDisconnect(*r.will)
	}()

	buf := bufio.NewReader(r.conn)

	for atomic.LoadUint32(&r.running) == 1 {
		var pkt packet.Provider
		if pkt, err = r.readPacket(buf); err != nil || pkt == nil {
			atomic.StoreUint32(&r.running, 0)
		} else {
			if r.keepAlive > 0 {
				r.conn.SetReadDeadline(time.Now().Add(r.keepAlive)) // nolint: errcheck
			}
			if err = r.onPacket(pkt); err != nil {
				atomic.StoreUint32(&r.running, 0)
			}
		}
	}
}

func (r *receiver) readPacket(buf *bufio.Reader) (packet.Provider, error) {
	var err error

	if len(r.recv) == 0 {
		var header []byte
		peekCount := 2
		// Let's read enough bytes to get the message header (msg type, remaining length)
		for {
			// If we have read 5 bytes and still not done, then there's a problem.
			if peekCount > 5 {
				return nil, errors.New("sendrecv/peekMessageSize: 4th byte of remaining length has continuation bit set")
			}

			header, err = buf.Peek(peekCount)
			if err != nil {
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

	offset := len(r.recv) - r.remainingRecv

	for offset != r.remainingRecv {
		var n int
		n, err = buf.Read(r.recv[offset:])
		offset += n
		if err != nil {
			return nil, err
		}
	}

	var pkt packet.Provider
	pkt, _, err = packet.Decode(r.version, r.recv)
	r.recv = []byte{}
	r.remainingRecv = 0

	return pkt, err
}
