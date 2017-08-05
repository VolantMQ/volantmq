package transport

import (
	"time"

	"go.uber.org/zap"

	"sync"

	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/routines"
	"github.com/troian/surgemq/session"
	"github.com/troian/surgemq/systree"
)

type TransportConfig struct {
	// AuthManager
	AuthManager *auth.Manager

	// Port tcp port to listen on
	Port int

	// Anonymous either allow or deny anonymous access
	// If not set than default is false
	Anonymous bool
}

type InternalConfig struct {
	// AllowedVersions what protocol version server will handle
	// If not set than defaults to 0x3 and 0x04
	AllowedVersions map[message.ProtocolVersion]bool

	// ConnectTimeout The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// KeepAlive The number of seconds to keep the connection live if there's no data.
	// If not set then defaults to 5 minutes.
	KeepAlive int

	Sessions *sessions.Manager

	Metric systree.Metric
}

type baseConfig struct {
	InternalConfig

	config TransportConfig

	quit chan struct{}
	log  *zap.Logger

	onStop       func(int)
	onConnection sync.WaitGroup
	once         struct {
		start sync.Once
		stop  sync.Once
	}
	protocol string
}

// Config listener
type Config interface{}

type Provider interface {
	Protocol() string
	Serve() error
	Close() error
	Port() int
}

func (c *baseConfig) Port() int {
	return c.config.Port
}

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

	var req message.Provider

	var buf []byte
	if buf, err = routines.GetMessageBuffer(conn); err != nil {
		c.log.Error("Couldn't get CONNECT message", zap.Error(err))
		return
	}

	if req, _, err = message.Decode(message.ProtocolV50, buf); err != nil {
		c.log.Warn("Couldn't decode message", zap.Error(err))

		if _, ok := err.(message.ReasonCode); ok {
			if req != nil {
				c.Metric.Packets().Received(req.Type())
			}
		}
	} else {
		switch r := req.(type) {
		case *message.ConnectMessage:
			m, _ := message.NewMessage(req.Version(), message.CONNACK)
			resp, _ := m.(*message.ConnAckMessage)

			// If protocol version is not in allowed list then give reject and pass control to session manager
			// to handle response
			if allowed, ok := c.AllowedVersions[r.Version()]; !ok || !allowed {
				reason := message.CodeRefusedUnacceptableProtocolVersion
				if r.Version() == message.ProtocolV50 {
					reason = message.CodeUnsupportedProtocol
				}
				resp.SetReturnCode(reason)
			} else {
				c.handleConnectionPermission(r, resp)
			}

			c.Sessions.Start(r, resp, conn)
		default:
			c.log.Error("Unexpected message type",
				zap.String("expected", "CONNECT"),
				zap.String("received", r.Type().Name()))
		}
	}
}

func (c *baseConfig) handleConnectionPermission(req *message.ConnectMessage, resp *message.ConnAckMessage) {
	user, pass := req.Credentials()

	var reason message.ReasonCode

	if len(user) > 0 {
		if err := c.config.AuthManager.Password(string(user), string(pass)); err == nil {
			reason = message.CodeSuccess
		} else {
			reason = message.CodeRefusedBadUsernameOrPassword
			if req.Version() == message.ProtocolV50 {
				reason = message.CodeBadUserOrPassword
			}
		}
	} else {
		if c.config.Anonymous {
			reason = message.CodeSuccess
		} else {
			reason = message.CodeRefusedBadUsernameOrPassword
			if req.Version() == message.ProtocolV50 {
				reason = message.CodeBadUserOrPassword
			}
		}
	}

	if req.KeepAlive() == 0 {
		req.SetKeepAlive(uint16(c.KeepAlive))
		resp.PropertySet(message.PropertyServerKeepAlive, uint16(c.KeepAlive)) // nolint: errcheck
	}

	resp.SetReturnCode(reason) // nolint: errcheck
}
