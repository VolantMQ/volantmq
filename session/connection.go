package session

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"errors"
	"sync"
	"sync/atomic"

	"github.com/troian/surgemq"
	"github.com/troian/surgemq/buffer"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
)

type onProcess struct {
	publish     func(msg *message.PublishMessage) error
	ack         func(msg message.Provider) error
	subscribe   func(msg *message.SubscribeMessage) error
	unSubscribe func(msg *message.UnSubscribeMessage) (*message.UnSubAckMessage, error)
	disconnect  func(will bool)
}

type connConfig struct {
	id            string
	keepAlive     int
	conn          io.Closer
	on            onProcess
	packetsMetric systree.PacketsMetric
}

type connection struct {
	config connConfig
	// Wait for the various goroutines to finish starting and stopping
	wg struct {
		routines struct {
			started sync.WaitGroup
			stopped sync.WaitGroup
		}
		conn struct {
			started sync.WaitGroup
			stopped sync.WaitGroup
		}
	}

	wmu sync.Mutex

	// Whether this connection is running or not.
	running int64

	// Quit signal for determining when this service should end. If channel is closed,
	// then exit.
	done chan struct{}

	// Incoming data buffer. Bytes are read from the connection and put in here.
	in *buffer.Type

	// Outgoing data buffer. Bytes written here are in turn written out to the connection.
	out *buffer.Type

	will bool

	log struct {
		prod *zap.Logger
		dev  *zap.Logger
	}
}

type netReader interface {
	io.Reader
	SetReadDeadline(t time.Time) error
}

type timeoutReader struct {
	d    time.Duration
	conn netReader
}

func newConnection(config connConfig) (conn *connection, err error) {
	conn = &connection{
		config: config,
		done:   make(chan struct{}),
		will:   true,
	}

	conn.log.prod = surgemq.GetProdLogger().Named("session.conn." + config.id)
	conn.log.dev = surgemq.GetDevLogger().Named("session.conn." + config.id)

	conn.wg.conn.started.Add(1)
	conn.wg.conn.stopped.Add(1)

	// Create the incoming ring buffer
	conn.in, err = buffer.New(buffer.DefaultBufferSize)
	if err != nil {
		return nil, err
	}

	// Create the outgoing ring buffer
	conn.out, err = buffer.New(buffer.DefaultBufferSize)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// start serving messages over this connection
func (s *connection) start() {
	// firstly check if connection already runnin
	if !atomic.CompareAndSwapInt64(&s.running, 0, 1) {
		// already running. Check if previous call to start has finished before return
		s.wg.conn.started.Wait()
		return
	}

	defer s.wg.conn.started.Done()

	s.wg.routines.stopped.Add(3)

	// these routines must start in specified order
	// and next proceed next one only when previous finished
	s.wg.routines.started.Add(1)
	go s.sender()
	s.wg.routines.started.Wait()

	s.wg.routines.started.Add(1)
	go s.processIncoming()
	s.wg.routines.started.Wait()

	s.wg.routines.started.Add(1)
	go s.receiver()
	s.wg.routines.started.Wait()
}

func (s *connection) stop() (ret bool) {
	// wait if stop invoked before start finished
	s.wg.conn.started.Wait()

	// if connection stop has already been invoked wait for previous call to finish
	if !atomic.CompareAndSwapInt64(&s.running, 1, 0) {
		// already running. Check if previous call to start has finished before return
		s.wg.conn.stopped.Wait()
		return false
	}

	// Close quit channel, effectively telling all the goroutines it's time to quit
	close(s.done)

	// Close the network connection
	// we do not check for error here as connection might be already closed by session
	s.config.conn.Close() // nolint: goling, errcheck, gas

	if err := s.in.Close(); err != nil {
		s.log.prod.Error("close input buffer error", zap.String("ClientID", s.config.id), zap.Error(err))
	}

	if err := s.out.Close(); err != nil {
		s.log.prod.Error("close output buffer error", zap.String("ClientID", s.config.id), zap.Error(err))
	}

	// Wait for all the connection goroutines are finished
	s.wg.routines.stopped.Wait()

	s.wg.conn.stopped.Done()

	defer func(will bool, onDisconnect func(will bool)) {
		onDisconnect(will)
	}(s.will, s.config.on.disconnect)

	return true
}

func (r timeoutReader) Read(b []byte) (int, error) {
	if err := r.conn.SetReadDeadline(time.Now().Add(r.d)); err != nil {
		return 0, err
	}
	return r.conn.Read(b)
}

func (s *connection) isDone() bool {
	select {
	case <-s.done:
		return true

	default:
	}

	return false
}

// reads message income messages
func (s *connection) processIncoming() {
	defer s.onRoutineReturn()

	s.wg.routines.started.Done()

	for {
		// 1. firstly lets peak message type and total length
		mType, total, err := s.peekMessageSize()

		if err != nil {
			if err != io.EOF {
				s.log.prod.Error("Error peeking next message size", zap.String("ClientID", s.config.id), zap.Error(err))
			}
			return
		}

		var msg message.Provider

		// 2. Now read message including fixed header
		msg, _, err = s.readMessage(mType, total)
		if err != nil {
			if err != io.EOF {
				s.log.prod.Error("Error peeking next message", zap.String("ClientID", s.config.id), zap.Error(err), zap.Int("total len", total))
			}
			return
		}

		s.config.packetsMetric.Received(msg.Type())

		// 3. Put message for further processing
		var resp message.Provider
		switch m := msg.(type) {
		case *message.PublishMessage:
			err = s.config.on.publish(m)
		case *message.PubAckMessage:
			err = s.config.on.ack(msg)
		case *message.PubRecMessage:
			err = s.config.on.ack(msg)
		case *message.PubRelMessage:
			err = s.config.on.ack(msg)
		case *message.PubCompMessage:
			err = s.config.on.ack(msg)
		case *message.SubscribeMessage:
			err = s.config.on.subscribe(m)
		case *message.UnSubscribeMessage:
			resp, _ = s.config.on.unSubscribe(m)
			_, err = s.writeMessage(resp)
		//case *message.UnSubAckMessage:
		//	// For UNSUBACK message, we should send to ack queue
		//	s.config.ackQueues.unSubAck.Ack(msg) // nolint: errcheck
		//	s.processAcked(s.config.ackQueues.unSubAck)
		case *message.PingReqMessage:
			// For PINGREQ message, we should send back PINGRESP
			resp = message.NewPingRespMessage()
			_, err = s.writeMessage(resp)
		case *message.DisconnectMessage:
			// For DISCONNECT message, we should quit without sending Will
			s.will = false
			return
		default:
			s.log.prod.Error("Unsupported incoming message type", zap.String("ClientID", s.config.id), zap.String("type", msg.Type().String()))
			return
		}

		if err != nil {
			return
		}

		if s.isDone() && s.in.Len() == 0 {
			return
		}
	}
}

// receiver reads data from the network, and writes the data into the incoming buffer
func (s *connection) receiver() {
	defer s.onRoutineReturn()

	s.wg.routines.started.Done()

	switch conn := s.config.conn.(type) {
	case net.Conn:
		keepAlive := time.Second * time.Duration(s.config.keepAlive)
		r := timeoutReader{
			d:    keepAlive + (keepAlive / 2),
			conn: conn,
		}

		for {
			if _, err := s.in.ReadFrom(r); err != nil {
				return
			}
		}
	default:
		s.log.prod.Error("Invalid connection type", zap.String("ClientID", s.config.id))
	}
}

// sender writes data from the outgoing buffer to the network
func (s *connection) sender() {
	defer s.onRoutineReturn()

	s.wg.routines.started.Done()

	switch conn := s.config.conn.(type) {
	case net.Conn:
		for {
			if _, err := s.out.WriteTo(conn); err != nil {
				return
			}
		}
	default:
		s.log.prod.Error("Invalid connection type", zap.String("ClientID", s.config.id))
	}
}

func (s *connection) onRoutineReturn() {
	s.wg.routines.stopped.Done()
	s.stop()

	if r := recover(); r != nil {
		s.log.prod.Error("Recover from panic")
		//debug.PrintStack()
	}
}

// peekMessageSize reads, but not commits, enough bytes to determine the size of
// the next message and returns the type and size.
func (s *connection) peekMessageSize() (message.Type, int, error) {
	var b []byte
	var err error
	cnt := 2

	if s.in == nil {
		err = types.ErrBufferNotReady
		return 0, 0, err
	}

	// Let's read enough bytes to get the message header (msg type, remaining length)
	for {
		// If we have read 5 bytes and still not done, then there's a problem.
		if cnt > 5 {
			return 0, 0, errors.New("sendrecv/peekMessageSize: 4th byte of remaining length has continuation bit set")
		}

		// Peek cnt bytes from the input buffer.
		b, err = s.in.ReadWait(cnt)
		if err != nil {
			return 0, 0, err
		}

		// If not enough bytes are returned, then continue until there's enough.
		if len(b) < cnt {
			continue
		}

		// If we got enough bytes, then check the last byte to see if the continuation
		// bit is set. If so, increment cnt and continue peeking
		if b[cnt-1] >= 0x80 {
			cnt++
		} else {
			break
		}
	}

	// Get the remaining length of the message
	remLen, m := binary.Uvarint(b[1:])

	// Total message length is remlen + 1 (msg type) + m (remlen bytes)
	total := int(remLen) + 1 + m

	mType := message.Type(b[0] >> 4)

	return mType, total, err
}

// peekMessage reads a message from the buffer, but the bytes are NOT committed.
// This means the buffer still thinks the bytes are not read yet.
//func (s *connection) peekMessage(mtype message.Type, total int) (message.Provider, int, error) {
//
//	var b []byte
//	var err error
//	var i int
//	var n int
//	var msg message.Provider
//
//	if s.in == nil {
//		return nil, 0, types.ErrBufferNotReady
//	}
//
//	// Peek until we get total bytes
//	for i = 0; ; i++ {
//		// Peek remLen bytes from the input buffer.
//		b, err = s.in.ReadWait(total)
//		if err != nil && err != buffer.ErrBufferInsufficientData {
//			return nil, 0, err
//		}
//
//		// If not enough bytes are returned, then continue until there's enough.
//		if len(b) >= total {
//			break
//		}
//	}
//
//	msg, err = mtype.New()
//	if err != nil {
//		return nil, 0, err
//	}
//
//	n, err = msg.Decode(b)
//	return msg, n, err
//}

// readMessage reads and copies a message from the buffer. The buffer bytes are
// committed as a result of the read.
func (s *connection) readMessage(mType message.Type, total int) (message.Provider, int, error) {
	defer func() {
		if int64(len(s.in.ExternalBuf)) > s.in.Size() {
			s.in.ExternalBuf = make([]byte, s.in.Size())
		}
	}()

	var err error
	var n int
	var msg message.Provider

	if s.in == nil {
		err = types.ErrBufferNotReady
		return nil, 0, err
	}

	if len(s.in.ExternalBuf) < total {
		s.in.ExternalBuf = make([]byte, total)
	}

	// Read until we get total bytes
	l := 0
	toRead := total
	for l < total {
		n, err = s.in.Read(s.in.ExternalBuf[l : l+toRead])
		l += n
		toRead -= n
		if err != nil {
			return nil, 0, err
		}
	}

	msg, err = mType.New()
	if err != nil {
		s.log.prod.Error("Error", zap.Error(err))
		return msg, 0, err
	}

	n, err = msg.Decode(s.in.ExternalBuf[:total])
	if err != nil {
		s.log.prod.Error("Error", zap.Error(err))
	}

	return msg, n, err
}

// writeMessage writes a message to the outgoing buffer
func (s *connection) writeMessage(msg message.Provider) (int, error) {
	// FIXME: Try to find a better way than a mutex...if possible.
	// This is to serialize writes to the underlying buffer. Multiple goroutines could
	// potentially get here because of calling Publish() or Subscribe() or other
	// functions that will send messages. For example, if a message is received in
	// another connection, and the message needs to be published to this client, then
	// the Publish() function is called, and at the same time, another client could
	// do exactly the same thing.
	//
	// Not an ideal fix though. If possible we should remove mutex and be lockfree.
	// Mainly because when there's a large number of goroutines that want to publish
	// to this client, then they will all block. However, this will do for now.
	s.wmu.Lock()
	defer s.wmu.Unlock()

	if s.out == nil {
		return 0, types.ErrBufferNotReady
	}

	var total int
	var err error

	total, err = msg.Send(s.out)

	if err == nil {
		s.config.packetsMetric.Sent(msg.Type())
	}

	return total, err
}
