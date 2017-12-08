package transport

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/VolantMQ/volantmq/configuration"
	"go.uber.org/zap"
	"runtime/debug"
	"fmt"
)

// ConfigTCP configuration of tcp transport
type ConfigTCP struct {
	Scheme    string
	Host      string
	CertFile  string
	KeyFile   string
	transport *Config
}

type tcp struct {
	baseConfig

	listener  net.Listener
	tlsConfig *tls.Config
}

// NewConfigTCP allocate new transport config for tcp transport
// Use of this function is preferable instead of direct allocation of ConfigTCP
func NewConfigTCP(transport *Config) *ConfigTCP {
	return &ConfigTCP{
		Scheme:    "tcp",
		Host:      "",
		transport: transport,
	}
}

// NewTCP create new tcp transport
func NewTCP(config *ConfigTCP, internal *InternalConfig) (Provider, error) {
	l := &tcp{}

	l.quit = make(chan struct{})
	l.protocol = config.Scheme
	l.InternalConfig = *internal
	l.config = *config.transport
	l.log = configuration.GetLogger().Named("server.transport.tcp")

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
	if ln, err = net.Listen(config.Scheme, config.Host+":"+config.transport.Port); err != nil {
		return nil, err
	}

	if l.tlsConfig != nil {
		l.listener = NewTLSListener(ln, l.tlsConfig)
	} else {
		l.listener = ln
	}

	return l, nil
}

// Close
func (l *tcp) Close() error {
	var err error

	l.onceStop.Do(func() {
		close(l.quit)

		err = l.listener.Close()
		l.onConnection.Wait()
	})

	return err
}

//func (l *tcp) Protocol() string {
//	return "tcp"
//}

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

		l.log.Debug("Accepted new connection.", zap.Stringer("client_addr", conn.RemoteAddr()))
		l.onConnection.Add(1)
		go func(cn net.Conn) {
			defer func(){
				sysErr := recover()
				if sysErr != nil {
					sysErr = fmt.Errorf("%v. trace: %s", sysErr, debug.Stack())
					l.log.Error("Sys err: connection handling routine crashed.", zap.Error(sysErr.(error)))
				}
				l.onConnection.Done()
			}()

			if conn, err := newConnTCP(cn, l.Metric.Bytes()); err != nil {
				l.log.Error("Couldn't create connection interface", zap.Error(err))
			} else {
				l.handleConnection(conn)
			}
		}(conn)
	}
}
