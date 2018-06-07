package transport

import (
	"context"
	"net/http"
	"time"

	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type httpServer struct {
	mux  *http.ServeMux
	http *http.Server
}

// ConfigWS listener object for websocket server
type ConfigWS struct {
	// CertFile
	CertFile string

	// KeyFile
	KeyFile string

	// Path
	Path string

	// SubProtocols
	SubProtocols []string

	transport *Config
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
	l.InternalConfig = *internal
	l.config = *config.transport

	if len(l.certFile) != 0 {
		l.protocol = "wss"
	} else {
		l.protocol = "ws"
	}

	l.log = configuration.GetLogger().Named("listener: " + l.protocol + "://:" + config.transport.Port)

	if len(config.Path) == 0 {
		config.Path = "/"
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

// ServeHTTP http connection upgrade
func (s *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// NewConnWs initiate connection with websocket.Conn ws object and stat
func (l *ws) newConnWs(conn *websocket.Conn, stat systree.BytesMetric) (Conn, error) {
	c := &connWs{
		conn: conn,
		stat: stat,
	}

	return c, nil
}

func (l *ws) serveWs(w http.ResponseWriter, r *http.Request) {
	cn, err := l.up.Upgrade(w, r, nil)
	if err != nil {
		l.log.Error("Couldn't upgrade WebSocket connection", zap.Error(err))
		return
	}

	l.onConnection.Add(1)
	go func(cn *websocket.Conn) {
		//defer l.onConnection.Done()
		//if inConn, e := newConnWs(cn, l.Metric.Bytes()); e != nil {
		//	l.log.Error("Couldn't create connection interface", zap.Error(e))
		//} else {
		//	l.handleConnection(inConn)
		//}
	}(cn)
}

func (l *ws) Ready() error {
	return nil
}

func (l *ws) Alive() error {
	return nil
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

// Close websocket connection
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
