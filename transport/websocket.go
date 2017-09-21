package transport

import (
	"net/http"

	"crypto/tls"

	"context"
	"time"

	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type httpServer struct {
	mux  *http.ServeMux
	http *http.Server
}

// ConfigWS listener object for websocket server
type ConfigWS struct {
	transport *Config

	// AuthManager
	AuthManager *auth.Manager

	// CertFile
	CertFile string

	// KeyFile
	KeyFile string

	// Path
	Path string

	// SubProtocols
	SubProtocols []string
}

type ws struct {
	baseConfig
	up       *websocket.Upgrader
	s        httpServer
	certFile string
	keyFile  string
}

// NewConfigWS allocate new transport config for websocket transport
// Use of this function is preferable instead of direct allocation of ConfigWS
func NewConfigWS(transport *Config) *ConfigWS {
	return &ConfigWS{
		Path:      "/",
		transport: transport,
	}
}

// NewWS create new websocket transport
func NewWS(config *ConfigWS, internal *InternalConfig) (Provider, error) {
	l := &ws{
		certFile: config.CertFile,
		keyFile:  config.KeyFile,
		up:       &websocket.Upgrader{},
	}

	l.quit = make(chan struct{})
	l.protocol = "ws"
	l.InternalConfig = *internal
	l.config = *config.transport
	l.log = configuration.GetLogger().Named("server.transport.ws")

	if len(config.Path) == 0 {
		config.Path = "/"
	}

	if len(config.CertFile) != 0 && len(config.KeyFile) != 0 {
		certificates := make([]tls.Certificate, 1)
		var err error

		if certificates[0], err = tls.LoadX509KeyPair(config.CertFile, config.KeyFile); err != nil {
			return nil, err
		}
	}

	l.up.Subprotocols = config.SubProtocols

	l.s.mux = http.NewServeMux()
	l.s.mux.HandleFunc(config.Path, l.serveWs)

	l.s.http = &http.Server{
		Addr:    ":" + config.transport.Port,
		Handler: &l.s,
	}

	return l, nil
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (l *ws) serveWs(w http.ResponseWriter, r *http.Request) {
	conn, err := l.up.Upgrade(w, r, nil)
	if err != nil {
		l.log.Error("Couldn't upgrade WebSocket connection", zap.Error(err))
		return
	}

	l.onConnection.Add(1)
	go func(cn *websocket.Conn) {
		defer l.onConnection.Done()
		if conn, err := newConnWs(cn, l.Metric.Bytes()); err != nil {
			l.log.Error("Couldn't create connection interface", zap.Error(err))
		} else {
			l.handleConnection(conn)
		}
	}(conn)
}

func (l *ws) Serve() error {
	var e error
	if len(l.certFile) != 0 && len(l.keyFile) != 0 {
		e = l.s.http.ListenAndServeTLS(l.certFile, l.keyFile)
	} else {
		e = l.s.http.ListenAndServe()
	}

	return e
}

func (l *ws) Close() error {
	var err error

	l.onceStop.Do(func() {
		ctx, ctxCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ctxCancel()

		err = l.s.http.Shutdown(ctx)
		l.onConnection.Wait()
	})

	return err
}

func (l *ws) Protocol() string {
	return "ws"
}
