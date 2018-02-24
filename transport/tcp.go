package transport

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/VolantMQ/volantmq/configuration"
	"go.uber.org/zap"
)

// ConfigTCP configuration of tcp transport
type ConfigTCP struct {
	Scheme    string
	TLS       *tls.Config
	transport *Config
}

type tcp struct {
	baseConfig

	listener net.Listener
}

// NewConfigTCP allocate new transport config for tcp transport
// Use of this function is preferable instead of direct allocation of ConfigTCP
func NewConfigTCP(transport *Config) *ConfigTCP {
	return &ConfigTCP{
		Scheme:    "tcp",
		transport: transport,
	}
}

// NewTCP create new tcp transport
func NewTCP(config *ConfigTCP, internal *InternalConfig) (Provider, error) {
	l := &tcp{}

	l.quit = make(chan struct{})
	l.InternalConfig = *internal
	l.config = *config.transport

	var err error
	var ln net.Listener

	if ln, err = net.Listen(config.Scheme, config.transport.Host+":"+config.transport.Port); err != nil {
		return nil, err
	}

	if config.TLS != nil {
		l.protocol = "ssl"
		l.listener = tls.NewListener(ln, config.TLS)
	} else {
		l.protocol = "tcp"
		l.listener = ln
	}

	l.log = configuration.GetLogger().Named("listener: " + l.protocol + "://:" + config.transport.Port)

	return l, nil
}

// Close tcp listener
func (l *tcp) Close() error {
	var err error

	l.onceStop.Do(func() {
		close(l.quit)

		err = l.listener.Close()
		l.onConnection.Wait()

		l.listener = nil
		l.log = nil
	})

	return err
}

// Serve start serving connections
func (l *tcp) Serve() error {
	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		var cn net.Conn
		var err error

		if cn, err = l.listener.Accept(); err != nil {
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

			if inConn, e := newConnTCP(cn, l.Metric.Bytes()); e != nil {
				l.log.Error("Couldn't create connection interface", zap.Error(e))
			} else {
				l.handleConnection(inConn)
			}
		}(cn)
	}
}
