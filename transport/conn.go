package transport

import (
	"errors"
	"net"
	"os"

	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/systree"
)

// Conn is wrapper to net.Conn
// implemented to encapsulate bytes statistic
type Conn interface {
	net.Conn
}

type conn struct {
	net.Conn
	stat systree.BytesMetric
}

var _ Conn = (*conn)(nil)

// Handler ...
type Handler interface {
	OnConnection(Conn, *auth.Manager) error
}

func newConn(cn net.Conn, stat systree.BytesMetric) (*conn, error) {
	c := &conn{
		Conn: cn,
		stat: stat,
	}

	return c, nil
}

// Read ...
func (c *conn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)

	c.stat.Received(uint64(n))

	return n, err
}

// Write ...
func (c *conn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.stat.Sent(uint64(n))

	return n, err
}

// File ...
func (c *conn) File() (*os.File, error) {
	switch t := c.Conn.(type) {
	case *net.TCPConn:
		return t.File()
	}

	return nil, errors.New("not implemented")
}
