package transport

import (
	"io"
	"net"
	"time"

	"github.com/VolantMQ/volantmq/systree"
	"github.com/gorilla/websocket"
)

type connWs struct {
	conn *websocket.Conn
	stat systree.BytesMetric
	prev io.Reader
	//pollDesc
}

var _ Conn = (*connWs)(nil)

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
