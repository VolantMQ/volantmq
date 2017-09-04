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

var _ conn = (*connTCP)(nil)

// NewConnTCP initiate connection with net.Conn tcp object and stat
func newConnTCP(conn net.Conn, stat systree.BytesMetric) (conn, error) {
	c := &connTCP{
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

func (c *connTCP) File() (*os.File, error) {
	if conn, ok := c.conn.(*net.TCPConn); ok {
		return conn.File()
	}

	return nil, errors.New("not implemented")
}
