package transport

import (
	"errors"
	"sync"
	"time"

	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/clients"
	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/routines"
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
	// AllowedVersions what protocol version server will handle
	// If not set than defaults to 0x3 and 0x04
	AllowedVersions map[packet.ProtocolVersion]bool

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

	var err error

	defer func() {
		if err != nil {
			conn.Close() // nolint: errcheck, gas
		}
	}()

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
	conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(c.ConnectTimeout))) // nolint: errcheck, gas

	var req packet.Provider

	var buf []byte
	if buf, err = routines.GetMessageBuffer(conn); err != nil {
		c.log.Error("Couldn't get CONNECT message", zap.Error(err))
		return
	}

	if req, _, err = packet.Decode(packet.ProtocolV50, buf); err != nil {
		c.log.Warn("Couldn't decode message", zap.Error(err))

		if _, ok := err.(packet.ReasonCode); ok {
			if req != nil {
				c.Metric.Packets().Received(req.Type())
			}
		}
	} else {
		// Disable read deadline. Will set it later if keep-alive interval is bigger than 0
		conn.SetReadDeadline(time.Time{}) // nolint: errcheck
		switch r := req.(type) {
		case *packet.Connect:
			m, _ := packet.NewMessage(req.Version(), packet.CONNACK)
			resp, _ := m.(*packet.ConnAck)

			var reason packet.ReasonCode
			// If protocol version is not in allowed list then give reject and pass control to session manager
			// to handle response
			if allowed, ok := c.AllowedVersions[r.Version()]; !ok || !allowed {
				reason = packet.CodeRefusedUnacceptableProtocolVersion
				if r.Version() == packet.ProtocolV50 {
					reason = packet.CodeUnsupportedProtocol
				}
			} else {
				user, pass := r.Credentials()

				if status := c.config.AuthManager.Password(string(user), string(pass)); status == auth.StatusAllow {
					reason = packet.CodeSuccess
					if r.KeepAlive() == 0 {
						r.SetKeepAlive(uint16(c.KeepAlive))
						resp.PropertySet(packet.PropertyServerKeepAlive, uint16(c.KeepAlive)) // nolint: errcheck
					}
				} else {
					reason = packet.CodeRefusedBadUsernameOrPassword
					if req.Version() == packet.ProtocolV50 {
						reason = packet.CodeBadUserOrPassword
					}
				}
			}
			resp.SetReturnCode(reason) // nolint: errcheck

			c.Sessions.NewSession(
				&clients.StartConfig{
					Req:  r,
					Resp: resp,
					Conn: conn,
					Auth: c.config.AuthManager,
				})
		default:
			c.log.Error("Unexpected message type",
				zap.String("expected", "CONNECT"),
				zap.String("received", r.Type().Name()))
			err = errors.New("unexpected message type")
		}
	}
}
