package server

import (
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
)

// ListenerTCP listener object for tcp server
type ListenerTCP struct {
	ListenerBase

	Scheme    string
	Host      string
	listener  net.Listener
	tlsConfig *tls.Config
}

// ListenAndServeTcp listens to connections on the URI requested, and handles any
// incoming MQTT client sessions. It should not return until Close() is called
// or if there's some critical error that stops the server from running. The URI
// supplied should be of the form "protocol://host:port" that can be parsed by
// url.Parse(). For example, an URI could be "tcp://0.0.0.0:1883".
func (l *ListenerTCP) start() error {
	select {
	case <-l.inner.quit:
		return nil
	default:
	}

	defer l.inner.lock.Unlock()
	l.inner.lock.Lock()

	var err error

	if l.CertFile != "" && l.KeyFile != "" {
		l.tlsConfig = &tls.Config{
			Certificates: make([]tls.Certificate, 1),
		}

		l.tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(l.CertFile, l.KeyFile)
		if err != nil {
			l.tlsConfig = nil
			return err
		}
	}

	var ln net.Listener
	if ln, err = net.Listen(l.Scheme, l.Host+":"+strconv.Itoa(l.Port)); err != nil {
		return err
	}

	if l.tlsConfig != nil {
		l.listener = tls.NewListener(ln, l.tlsConfig)
	} else {
		l.listener = ln
	}

	if _, ok := l.inner.listeners.list[l.Port]; !ok {
		//listener.quit = s.quit
		l.inner.listeners.list[l.Port] = l
		l.inner.listeners.wg.Add(1)

		go func() {
			defer l.inner.listeners.wg.Done()

			if l.inner.config.ListenerStatus != nil {
				l.inner.config.ListenerStatus(l.Scheme+"://"+l.Host+":"+strconv.Itoa(l.Port), true)
			}
			err = l.serve()

			if l.inner.config.ListenerStatus != nil {
				l.inner.config.ListenerStatus(l.Scheme+"://"+l.Host+":"+strconv.Itoa(l.Port), false)
			}
		}()
	} else {
		err = errors.New("Listener already exists")
	}

	return err
}

func (l *ListenerTCP) close() error {
	return l.listener.Close()
}

func (l *ListenerTCP) listenerProtocol() string {
	return "tcp"
}

func (l *ListenerTCP) serve() error {
	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		var conn net.Conn
		var err error

		if conn, err = l.listener.Accept(); err != nil {
			// http://zhen.org/blog/graceful-shutdown-of-go-net-dot-listeners/
			select {
			case <-l.inner.quit:
				return nil
			default:
			}

			// Borrowed from go1.3.3/src/pkg/net/http/server.go:1699
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				l.log.Prod.Error("Couldn't accept connection. Retrying", zap.Error(err), zap.Duration("retryIn", tempDelay))

				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		l.inner.wgConnections.Add(1)
		go func(cn net.Conn) {
			defer l.inner.wgConnections.Done()

			if conn, err := types.NewConnTCP(cn, l.inner.sysTree.Metric().Bytes()); err != nil {
				l.log.Prod.Error("Couldn't create connection interface", zap.Error(err))
			} else {
				l.handleConnection(conn) // nolint: errcheck, gas
			}
		}(conn)
	}
}
