package transport

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/VolantMQ/volantmq/types"

	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/systree"
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

// Ready ...
func (l *tcp) Ready() error {
	if err := l.baseReady(); err != nil {
		return err
	}

	return nil
}

// Alive ...
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

		l.listener = nil
		l.log = nil
	})

	return err
}

func (l *tcp) newConn(cn net.Conn, stat systree.BytesMetric) Conn {
	c := newConn(cn, stat)

	if l.tls != nil {
		c.Conn = tls.Server(cn, l.tls)
	}

	return c
}

// Serve start serving connections
func (l *tcp) Serve() error {

	// accept is a channel to signal about next incoming connection Accept()
	// results.
	accept := make(chan error, 1)

	defer close(accept)

	for {
		// Try to accept incoming connection inside free pool worker.
		// If there no free workers for 1ms, do not accept anything and try later.
		// This will help us to prevent many self-ddos or out of resource limit cases.
		err := l.AcceptPool.ScheduleTimeout(time.Millisecond, func() {
			var cn net.Conn
			var er error

			defer func() {
				accept <- er
			}()

			if cn, er = l.listener.Accept(); er == nil {
				select {
				case <-l.quit:
					er = types.ErrClosed
					return
				default:
				}

				l.handleConnection(l.newConn(cn, l.Metric.Bytes()))
			}
		})

		// We do not want to accept incoming connection when goroutine pool is
		// busy. So if there are no free goroutines during 1ms we want to
		// cooldown the server and do not receive connection for some short
		// time.
		if err != nil && err != types.ErrScheduleTimeout {
			break
		} else if err == types.ErrScheduleTimeout {
			continue
		}

		err = <-accept

		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				delay := 5 * time.Millisecond
				time.Sleep(delay)
			} else {
				// unknown error stop listener
				break
			}
		}
	}

	return nil
}
