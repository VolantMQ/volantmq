package transport

import (
	"sync"

	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/clients"
	"github.com/VolantMQ/volantmq/systree"
	"go.uber.org/zap"
)

// Config is base configuration object used by all transports
type Config struct {
	// AuthManager
	AuthManager *auth.Manager

	// Port tcp port to listen on
	Port string
}

// InternalConfig used by server implementation to configure internal specific needs
type InternalConfig struct {
	Sessions *clients.Manager

	Metric systree.Metric

	// ConnectTimeout The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// KeepAlive The number of seconds to keep the connection live if there's no data.
	// If not set then defaults to 5 minutes.
	KeepAlive int
}

type baseConfig struct {
	InternalConfig
	config       Config
	onConnection sync.WaitGroup // nolint: structcheck
	onceStop     sync.Once      // nolint: structcheck
	quit         chan struct{}  // nolint: structcheck
	log          *zap.Logger
	protocol     string
}

// Provider is interface that all of transports must implement
type Provider interface {
	Protocol() string
	Serve() error
	Close() error
	Port() string
}

// Port return tcp port used by transport
func (c *baseConfig) Port() string {
	return c.config.Port
}

// Protocol return protocol name used by transport
func (c *baseConfig) Protocol() string {
	return c.protocol
}

// handleConnection is for the broker to handle an incoming connection from a client
func (c *baseConfig) handleConnection(conn conn) {
	if c == nil {
		c.log.Error("Invalid connection type")
		return
	}

	// To establish a connection, we must
	// 1. Read and decode the message.ConnectMessage from the wire
	// 2. If no decoding errors, then authenticate using username and password.
	//    Otherwise, write out to the wire message.ConnackMessage with
	//    appropriate error.
	// 3. If authentication is successful, then either create a new session or
	//    retrieve existing session
	// 4. Write out to the wire a successful message.ConnackMessage message

	// Read the CONNECT message from the wire, if error, then check to see if it's
	// a CONNACK error. If it's CONNACK error, send the proper CONNACK error back
	// to client. Exit regardless of error type.
	//conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(c.ConnectTimeout))) // nolint: errcheck, gas

	if err := c.Sessions.Handle(conn, c.config.AuthManager); err != nil {
		conn.Close() // nolint: errcheck, gas
	}
}
