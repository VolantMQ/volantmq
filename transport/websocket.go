package transport

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/gobwas/ws/wsutil"
	"github.com/gorilla/websocket"

	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/systree"
)

type httpServer struct {
	mux  *http.ServeMux
	http *http.Server
}

type wsConn struct {
	*conn
	rem []byte
}

// Read ...
func (c *wsConn) Read(b []byte) (int, error) {
	var n int
	if len(c.rem) > 0 {
		n = copy(b, c.rem)

		if n < len(c.rem) {
			c.rem = c.rem[n:]
		}
	}

	var err error
	if n < len(b) {
		var data []byte
		data, err = wsutil.ReadClientBinary(c.Conn)
		n1 := copy(b[n:], data)
		n += n1
		if n1 < len(data) {
			c.rem = data[n1:]
		}
	}

	c.stat.Received(uint64(n))

	return n, err
}

// Write ...
func (c *wsConn) Write(b []byte) (int, error) {
	err := wsutil.WriteServerBinary(c.conn, b)
	n := 0
	if err == nil {
		n = len(b)
		c.stat.Sent(uint64(n))
	}

	return n, err
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
	httpServer
	up       websocket.Upgrader
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

	l.mux = http.NewServeMux()
	l.mux.HandleFunc(config.Path, l.serve)

	l.http = &http.Server{
		Addr:    ":" + config.transport.Port,
		Handler: l,
	}

	return l, nil
}

func (l *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mux.ServeHTTP(w, r)
}

func (l *ws) newConn(cn net.Conn, stat systree.BytesMetric) (Conn, error) {
	wr, err := newConn(cn, stat)
	if err != nil {
		return nil, err
	}

	c := &wsConn{
		conn: wr,
	}

	return c, nil
}

func (l *ws) serve(w http.ResponseWriter, r *http.Request) {
	// cn, err := l.up.Upgrade(w, r, nil)
	// if err != nil {
	// 	l.log.Errorf("upgrade error: %s", err)
	// 	return
	// }
	//
	// l.onConnection.Add(1)
	// go func() {
	// 	defer l.onConnection.Done()
	// 	if inConn, e := l.newConn(cn, l.Metric.Bytes()); e != nil {
	// 		l.log.Error("Couldn't create connection interface", zap.Error(e))
	// 	} else {
	// 		l.handleConnection(inConn)
	// 	}
	// }()
}

// Ready ...
func (l *ws) Ready() error {
	return nil
}

// Alive ...
func (l *ws) Alive() error {
	return nil
}

// Serve ...
func (l *ws) Serve() error {
	var e error
	if len(l.certFile) != 0 && len(l.keyFile) != 0 {
		e = l.http.ListenAndServeTLS(l.certFile, l.keyFile)
	} else {
		e = l.http.ListenAndServe()
	}

	return e
}

// Close websocket listener
func (l *ws) Close() error {
	var err error

	l.onceStop.Do(func() {
		ctx, ctxCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ctxCancel()

		err = l.http.Shutdown(ctx)
		l.onConnection.Wait()
	})

	return err
}
