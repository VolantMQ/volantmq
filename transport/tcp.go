package transport

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/systree"
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
	tls      *tls.Config
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
	l.tls = config.TLS

	var err error

	if l.listener, err = net.Listen(config.Scheme, config.transport.Host+":"+config.transport.Port); err != nil {
		return nil, err
	}

	if l.tls != nil {
		l.protocol = "ssl"
	} else {
		l.protocol = "tcp"

	}

	l.log = configuration.GetLogger().Named("listener: " + l.protocol + "://:" + config.transport.Port)

	return l, nil
}

func (l *tcp) Ready() error {
	if err := l.baseReady(); err != nil {
		return err
	}

	return nil
}

func (l *tcp) Alive() error {
	if err := l.baseReady(); err != nil {
		return err
	}

	return nil
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

// newConnTCP initiate connection with net.Conn tcp object and stat
func (l *tcp) newConnTCP(conn net.Conn, stat systree.BytesMetric) (Conn, error) {
	//desc, err := netpoll.HandleReadOnce(conn)
	//if err != nil {
	//	return nil, err
	//}

	newConn := &connTCP{
		conn: conn,
		stat: stat,
		//pollDesc: pollDesc{
		//	desc:  desc,
		//	ePoll: l.EventPoll,
		//},
	}

	if l.tls != nil {
		newConn.conn = tls.Server(conn, l.tls)
	}

	return newConn, nil
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
				l.log.Error("accept connection. Retrying",
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

			if inConn, e := l.newConnTCP(cn, l.Metric.Bytes()); e != nil {
				l.log.Error("create connection interface", zap.Error(e))
			} else {
				l.handleConnection(inConn)
			}
		}(cn)
	}
}
