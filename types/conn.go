package types

import (
	"github.com/troian/surgemq/systree"
	"net"
	"time"
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

type connImpl struct {
	conn net.Conn
	stat systree.BytesMetric
}

// NewConn initiate connection with net.Conn object and stat
func NewConn(conn net.Conn, stat systree.BytesMetric) (Conn, error) {
	c := &connImpl{
		conn: conn,
		stat: stat,
	}

	return c, nil
}

func (c *connImpl) Read(b []byte) (int, error) {
	n, err := c.conn.Read(b)

	c.stat.Received(uint64(n))

	return n, err
}

func (c *connImpl) Write(b []byte) (int, error) {
	n, err := c.conn.Write(b)
	c.stat.Sent(uint64(n))

	return n, err
}

func (c *connImpl) Close() error {
	return c.conn.Close()
}

func (c *connImpl) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *connImpl) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *connImpl) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *connImpl) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *connImpl) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
