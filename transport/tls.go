package transport

import (
	"net"
	"crypto/tls"
	"os"
	"errors"
)

func NewTLSListener(inner net.Listener, config *tls.Config) *TLSListener {
	l := new(TLSListener)
	l.Listener = inner
	l.config = config
	return l
}

// TLSListener wraps standard tls listener.
type TLSListener struct {
	net.Listener
	config *tls.Config
}

// TLSConnection wraps standard tls connection and implements the File function.
type TLSConnection struct {
	*tls.Conn
	innerConn net.Conn
}

// Accept the next incoming connection and wraps it into tls Conn.
func (l *TLSListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &TLSConnection{
		tls.Server(c, l.config),
		 c,
	}, nil
}

func (c *TLSConnection)File()(*os.File, error){
	switch c.innerConn.(type){
	case *net.TCPConn:
		return c.innerConn.(*net.TCPConn).File()
	case *net.UnixConn:
		return c.innerConn.(*net.UnixConn).File()
	case *net.UDPConn:
		return c.innerConn.(*net.UDPConn).File()
	}
	return nil, errors.New("connection type not recognized.")
}