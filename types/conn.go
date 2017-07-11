package types

import (
	"net"
	"time"

	"io"

	"github.com/gorilla/websocket"
	"github.com/troian/surgemq/systree"
)

// Conn is wrapper to net.Conn
// Implemented to incapsulate bytes statistic
type Conn interface {
	// Read reads data from the connection.
	// Read can be made to time out and return an Error with Timeout() == true
	// after a fixed time limit; see SetDeadline and SetReadDeadline.
	Read(b []byte) (n int, err error)

	// Write writes data to the connection.
	// Write can be made to time out and return an Error with Timeout() == true
	// after a fixed time limit; see SetDeadline and SetWriteDeadline.
	Write(b []byte) (n int, err error)

	// Close closes the connection.
	// Any blocked Read or Write operations will be unblocked and return errors.
	Close() error

	// LocalAddr returns the local network address.
	LocalAddr() net.Addr

	// RemoteAddr returns the remote network address.
	RemoteAddr() net.Addr

	// SetDeadline sets the read and write deadlines associated
	// with the connection. It is equivalent to calling both
	// SetReadDeadline and SetWriteDeadline.
	//
	// A deadline is an absolute time after which I/O operations
	// fail with a timeout (see type Error) instead of
	// blocking. The deadline applies to all future and pending
	// I/O, not just the immediately following call to Read or
	// Write. After a deadline has been exceeded, the connection
	// can be refreshed by setting a deadline in the future.
	//
	// An idle timeout can be implemented by repeatedly extending
	// the deadline after successful Read or Write calls.
	//
	// A zero value for t means I/O operations will not time out.
	SetDeadline(t time.Time) error

	// SetReadDeadline sets the deadline for future Read calls
	// and any currently-blocked Read call.
	// A zero value for t means Read will not time out.
	SetReadDeadline(t time.Time) error

	// SetWriteDeadline sets the deadline for future Write calls
	// and any currently-blocked Write call.
	// Even if write times out, it may return n > 0, indicating that
	// some of the data was successfully written.
	// A zero value for t means Write will not time out.
	SetWriteDeadline(t time.Time) error
}

type connTCP struct {
	conn net.Conn
	stat systree.BytesMetric
}

type connWs struct {
	conn *websocket.Conn
	stat systree.BytesMetric

	prev io.Reader
}

// NewConnTCP initiate connection with net.Conn tcp object and stat
func NewConnTCP(conn net.Conn, stat systree.BytesMetric) (Conn, error) {
	c := &connTCP{
		conn: conn,
		stat: stat,
	}

	return c, nil
}

// NewConnWs initiate connection with websocket.Conn ws object and stat
func NewConnWs(conn *websocket.Conn, stat systree.BytesMetric) (Conn, error) {
	c := &connWs{
		conn: conn,
		stat: stat,
	}

	return c, nil
}

func (c *connTCP) Read(b []byte) (int, error) {
	n, err := c.conn.Read(b)

	c.stat.Received(uint64(n))

	return n, err
}

func (c *connTCP) Write(b []byte) (int, error) {
	n, err := c.conn.Write(b)
	c.stat.Sent(uint64(n))

	return n, err
}

func (c *connTCP) Close() error {
	return c.conn.Close()
}

func (c *connTCP) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *connTCP) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *connTCP) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *connTCP) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *connTCP) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// Read
// FIXME: looks ugly
func (c *connWs) Read(b []byte) (int, error) {
	var mType int
	var err error
	var total int

	// if previous reader exists try read remaining data
	if c.prev != nil {
		total, err = c.prev.Read(b)

		if total > 0 {
			c.stat.Received(uint64(total))
		}
		if err != nil {
			c.prev = nil
		}

		if total > 0 {
			return total, nil
		}
	}

	if mType, c.prev, err = c.conn.NextReader(); err != nil {
		return 0, io.EOF
	}

	switch mType {
	case websocket.CloseMessage:
		return 0, io.EOF
	case websocket.TextMessage:
		fallthrough
	case websocket.PingMessage:
		fallthrough
	case websocket.PongMessage:
		return 0, nil
	}

	total, err = c.prev.Read(b)
	if total > 0 {
		c.stat.Received(uint64(total))
	}

	if err != nil {
		c.prev = nil
	}

	if total > 0 {
		return total, nil
	}

	return 0, nil
}

func (c *connWs) Write(b []byte) (int, error) {
	err := c.conn.WriteMessage(websocket.BinaryMessage, b)
	n := 0
	if err == nil {
		n = len(b)
		c.stat.Sent(uint64(n))
	}

	return n, err
}

func (c *connWs) Close() error {
	return c.conn.Close()
}

func (c *connWs) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *connWs) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *connWs) SetDeadline(t time.Time) error {
	if err := c.conn.SetReadDeadline(t); err != nil {
		return err
	}

	if err := c.conn.SetWriteDeadline(t); err != nil {
		return err
	}

	return nil
}

func (c *connWs) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *connWs) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
