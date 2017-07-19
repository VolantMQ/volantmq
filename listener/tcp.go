package listener

import (
	"crypto/tls"
	"net"
	"strconv"
	"time"

	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
)

// ConfigTCP listener object for tcp server
type ConfigTCP struct {
	InternalConfig

	// Scheme
	Scheme string

	// Host
	Host string

	// Port
	Port int

	// CertFile
	CertFile string

	// KeyFile
	KeyFile string

	// AuthManager
	AuthManager *auth.Manager
}

type tcp struct {
	baseConfig

	listener  net.Listener
	tlsConfig *tls.Config
}

func NewTCP(config *ConfigTCP) (Provider, error) {
	l := &tcp{}

	l.InternalConfig = config.InternalConfig
	l.quit = make(chan struct{})
	l.auth = config.AuthManager
	l.lPort = config.Port

	var err error

	if config.CertFile != "" && config.KeyFile != "" {
		l.tlsConfig = &tls.Config{
			Certificates: make([]tls.Certificate, 1),
		}

		l.tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			l.tlsConfig = nil
			return nil, err
		}
	}

	var ln net.Listener
	if ln, err = net.Listen(config.Scheme, config.Host+":"+strconv.Itoa(config.Port)); err != nil {
		return nil, err
	}

	if l.tlsConfig != nil {
		l.listener = tls.NewListener(ln, l.tlsConfig)
	} else {
		l.listener = ln
	}

	return l, nil
}

func (l *tcp) Close() error {
	var err error

	l.once.stop.Do(func() {
		close(l.quit)

		err = l.listener.Close()
		l.onConnection.Wait()
	})

	return err
}

func (l *tcp) Protocol() string {
	return "tcp"
}

func (l *tcp) Serve() error {
	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		var conn net.Conn
		var err error

		if conn, err = l.listener.Accept(); err != nil {
			// http://zhen.org/blog/graceful-shutdown-of-go-net-dot-listeners/
			select {
			case <-l.quit:
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
				l.log.Error("Couldn't accept connection. Retrying",
					zap.Error(err),
					zap.Duration("retryIn", tempDelay))

				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		l.onConnection.Add(1)
		go func(cn net.Conn) {
			defer l.onConnection.Done()

			if conn, err := types.NewConnTCP(cn, l.Metric); err != nil {
				l.log.Error("Couldn't create connection interface", zap.Error(err))
			} else {
				l.HandleConnection(conn, l.auth)
			}
		}(conn)
	}
}
