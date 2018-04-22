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
	//pollDesc
	//timer *time.Timer
}

var _ Conn = (*connTCP)(nil)

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
	//c.ePoll.Stop(c.desc)
	//c.desc.Close()
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
	//if t.IsZero() && c.timer != nil {
	//	if !c.timer.Stop() {
	//		return errors.New("keepAlive expired")
	//	}
	//} else {
	//	if c.timer == nil {
	//		c.timer = time.AfterFunc(t.Sub(time.Now()), c.keepAlive)
	//	} else {
	//		c.timer.Reset(t.Sub(time.Now()))
	//	}
	//}
	return c.conn.SetReadDeadline(t)
}

func (c *connTCP) keepAlive() {
	c.conn.Close()
}

func (c *connTCP) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *connTCP) File() (*os.File, error) {
	switch t := c.conn.(type) {
	case *net.TCPConn:
		return t.File()
	}

	return nil, errors.New("not implemented")
}
