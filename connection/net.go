package connection

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"errors"
	"sync"

	"github.com/troian/surgemq/buffer"
	"github.com/troian/surgemq/configuration"
	"github.com/troian/surgemq/packet"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
)

// onProcess callbacks to parent session
type onProcess struct {
	// Publish call when PUBLISH message received
	publish func(msg *packet.Publish) error

	// Ack call when PUBACK/PUBREC/PUBREL/PUBCOMP received
	ack func(msg packet.Provider) error

	// Subscribe call when SUBSCRIBE message received
	subscribe func(msg *packet.Subscribe) error

	// UnSubscribe call when UNSUBSCRIBE message received
	unSubscribe func(msg *packet.UnSubscribe) (*packet.UnSubAck, error)

	// Disconnect call when connection falls into error or received DISCONNECT message
	disconnect func(will bool)
}

// netConfig of connection
type netConfig struct {
	// On parent session callbacks
	on onProcess

	// Conn is network connection
	conn io.Closer

	// PacketsMetric interface to metric packets
	packetsMetric systree.PacketsMetric

	// ID ClientID
	id string

	// KeepAlive
	keepAlive uint16

	// ProtoVersion MQTT protocol version
	protoVersion packet.ProtocolVersion
}

// netConn implementation of the connection
type netConn struct {
	// Incoming data buffer. Bytes are read from the connection and put in here
	in *buffer.Type

	// Outgoing data buffer. Bytes written here are in turn written out to the connection
	out    *buffer.Type
	config *netConfig

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

	log struct {
		prod *zap.Logger
		dev  *zap.Logger
	}

	// Quit signal for determining when this service should end. If channel is closed, then exit
	expireIn *time.Duration
	done     chan struct{}
	wmu      sync.Mutex
	onStop   types.Once
	will     bool
}

type netReader interface {
	io.Reader
	SetReadDeadline(t time.Time) error
}

type timeoutReader struct {
	d    time.Duration
	conn netReader
}

// newNet connection
func newNet(config *netConfig) (f *netConn, err error) {
	defer func() {
		if err != nil {
			close(f.done)
		}
	}()

	f = &netConn{
		config: config,
		done:   make(chan struct{}),
		will:   true,
	}

	f.log.prod = configuration.GetProdLogger().Named("session.conn." + config.id)
	f.log.dev = configuration.GetDevLogger().Named("session.conn." + config.id)

	f.wg.conn.started.Add(1)
	f.wg.conn.stopped.Add(1)

	// Create the incoming ring buffer
	f.in, err = buffer.New(buffer.DefaultBufferSize)
	if err != nil {
		return nil, err
	}

	// Create the outgoing ring buffer
	f.out, err = buffer.New(buffer.DefaultBufferSize)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// Start serving messages over this connection
func (s *netConn) start() {
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

// Stop connection. Effective is only first invoke
func (s *netConn) stop(reason *packet.ReasonCode) {
	s.onStop.Do(func() {
		// wait if stop invoked before start finished
		s.wg.conn.started.Wait()

		// Close quit channel, effectively telling all the goroutines it's time to quit
		if !s.isDone() {
			close(s.done)
		}

		if s.config.protoVersion == packet.ProtocolV50 && reason != nil {
			m, _ := packet.NewMessage(packet.ProtocolV50, packet.DISCONNECT)
			msg, _ := m.(*packet.Disconnect)
			msg.SetReasonCode(*reason)

			// TODO: send it over
		}

		if err := s.config.conn.Close(); err != nil {
			s.log.prod.Error("close connection", zap.String("ClientID", s.config.id), zap.Error(err))
		}

		if err := s.in.Close(); err != nil {
			s.log.prod.Error("close input buffer error", zap.String("ClientID", s.config.id), zap.Error(err))
		}

		if err := s.out.Close(); err != nil {
			s.log.prod.Error("close output buffer error", zap.String("ClientID", s.config.id), zap.Error(err))
		}

		// Wait for all the connection goroutines are finished
		s.wg.routines.stopped.Wait()

		s.wg.conn.stopped.Done()

		s.config.on.disconnect(s.will)
	})
}

// Read
func (r timeoutReader) Read(b []byte) (int, error) {
	if err := r.conn.SetReadDeadline(time.Now().Add(r.d)); err != nil {
		return 0, err
	}
	return r.conn.Read(b)
}

// isDone
func (s *netConn) isDone() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// onRoutineReturn
func (s *netConn) onRoutineReturn() {
	s.wg.routines.stopped.Done()
	s.stop(nil)
}

// reads message income messages
func (s *netConn) processIncoming() {
	defer s.onRoutineReturn()

	s.wg.routines.started.Done()

	for !s.isDone() || s.in.Len() > 0 {
		// 1. firstly lets peak message type and total length
		mType, total, err := s.peekMessageSize()
		if err != nil {
			if err != io.EOF {
				s.log.prod.Error("Error peeking next message size", zap.String("ClientID", s.config.id), zap.Error(err))
			}
			return
		}

		if ok, e := mType.Valid(s.config.protoVersion); !ok {
			s.log.prod.Error("Invalid message type received", zap.String("ClientID", s.config.id), zap.Error(e))
			return
		}

		var msg packet.Provider

		// 2. Now read message including fixed header
		msg, _, err = s.readMessage(total)
		if err != nil {
			if err != io.EOF {
				s.log.prod.Error("Couldn't read message",
					zap.String("ClientID", s.config.id),
					zap.Error(err),
					zap.Int("total len", total))
			}
			return
		}

		s.config.packetsMetric.Received(msg.Type())

		// 3. Put message for further processing
		var resp packet.Provider
		switch m := msg.(type) {
		case *packet.Publish:
			err = s.config.on.publish(m)
		case *packet.Ack:
			err = s.config.on.ack(msg)
		case *packet.Subscribe:
			err = s.config.on.subscribe(m)
		case *packet.UnSubscribe:
			resp, _ = s.config.on.unSubscribe(m)
			_, err = s.WriteMessage(resp, false)
		case *packet.PingReq:
			// For PINGREQ message, we should send back PINGRESP
			mR, _ := packet.NewMessage(s.config.protoVersion, packet.PINGRESP)
			resp, _ := mR.(*packet.PingResp)
			_, err = s.WriteMessage(resp, false)
		case *packet.Disconnect:
			// For DISCONNECT message, we should quit without sending Will
			s.will = false

			if s.config.protoVersion == packet.ProtocolV50 {
				// FIXME: CodeRefusedBadUsernameOrPassword has same id as CodeDisconnectWithWill
				if m.ReasonCode() == packet.CodeRefusedBadUsernameOrPassword {
					s.will = true
				}

				expireIn := time.Duration(0)
				if val, e := m.PropertyGet(packet.PropertySessionExpiryInterval); e == nil {
					expireIn = time.Duration(val.(uint32))
				}

				// If the Session Expiry Interval in the CONNECT packet was zero, then it is a Protocol Error to set a non-
				// zero Session Expiry Interval in the DISCONNECT packet sent by the Client. If such a non-zero Session
				// Expiry Interval is received by the Server, it does not treat it as a valid DISCONNECT packet. The Server
				// uses DISCONNECT with Reason Code 0x82 (Protocol Error) as described in section 4.13.
				if s.expireIn != nil && *s.expireIn == 0 && expireIn != 0 {
					m, _ := packet.NewMessage(packet.ProtocolV50, packet.DISCONNECT)
					msg, _ := m.(*packet.Disconnect)
					msg.SetReasonCode(packet.CodeProtocolError)
					s.WriteMessage(msg, true) // nolint: errcheck
				}
			}
			return
		default:
			s.log.prod.Error("Unsupported incoming message type",
				zap.String("ClientID", s.config.id),
				zap.String("type", msg.Type().Name()))
			return
		}

		if err != nil {
			return
		}
	}
}

// receiver reads data from the network, and writes the data into the incoming buffer
func (s *netConn) receiver() {
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
func (s *netConn) sender() {
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

// peekMessageSize reads, but not commits, enough bytes to determine the size of
// the next message and returns the type and size.
func (s *netConn) peekMessageSize() (packet.Type, int, error) {
	var b []byte
	var err error
	cnt := 2

	if s.in == nil {
		err = buffer.ErrNotReady
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

	mType := packet.Type(b[0] >> 4)

	return mType, total, err
}

// readMessage reads and copies a message from the buffer. The buffer bytes are
// committed as a result of the read.
func (s *netConn) readMessage(total int) (packet.Provider, int, error) {
	defer func() {
		if int64(len(s.in.ExternalBuf)) > s.in.Size() {
			s.in.ExternalBuf = make([]byte, s.in.Size())
		}
	}()

	var err error
	var n int
	var msg packet.Provider

	if s.in == nil {
		err = buffer.ErrNotReady
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

	var dTotal int
	if msg, dTotal, err = packet.Decode(s.config.protoVersion, s.in.ExternalBuf[:total]); err == nil && total != dTotal {
		s.log.prod.Error("Incoming and outgoing length does not match",
			zap.Int("in", total),
			zap.Int("out", dTotal))
		return nil, 0, buffer.ErrNotReady
	}

	return msg, n, err
}

// WriteMessage writes a message to the outgoing buffer
func (s *netConn) WriteMessage(msg packet.Provider, lastMessage bool) (int, error) {
	if s.isDone() {
		return 0, buffer.ErrNotReady
	}

	if lastMessage {
		close(s.done)
	}

	defer s.wmu.Unlock()
	s.wmu.Lock()

	if s.out == nil {
		return 0, buffer.ErrNotReady
	}

	var total int
	var err error

	if total, err = packet.WriteToBuffer(msg, s.out); err == nil {
		s.config.packetsMetric.Sent(msg.Type())
	}

	return total, err
}
