package transport

import (
	"errors"
	"net"
	"os"
	"time"

	"github.com/VolantMQ/volantmq/systree"
)

type connTCP struct {
	conn net.Conn
	stat systree.BytesMetric
}

var _ Conn = (*connTCP)(nil)

// Read ...
func (c *connTCP) Read(b []byte) (int, error) {
	n, err := c.conn.Read(b)

	c.stat.Received(uint64(n))

	return n, err
}

// Write ...
func (c *connTCP) Write(b []byte) (int, error) {
	n, err := c.conn.Write(b)
	c.stat.Sent(uint64(n))

	return n, err
}

// Close ...
func (c *connTCP) Close() error {
	return c.conn.Close()
}

// LocalAddr ...
func (c *connTCP) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr ...
func (c *connTCP) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline ...
func (c *connTCP) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// SetReadDeadline ...
func (c *connTCP) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline ...
func (c *connTCP) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// File ...
func (c *connTCP) File() (*os.File, error) {
	switch t := c.conn.(type) {
	case *net.TCPConn:
		return t.File()
	}

	return nil, errors.New("not implemented")
}
