package transport

import (
	"context"
	"net"
	"net/http"
	"regexp"
	"time"

	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/metrics"
)

var subProtocolRegexp = regexp.MustCompile(`^mqtt(([vV])(3.1|3.1.1|5.0))?$`)

type httpServer struct {
	http *http.Server     // nolint:structcheck
	up   gws.HTTPUpgrader // nolint:structcheck
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

	c.stat.OnRecv(n)

	return n, err
}

// Write ...
func (c *wsConn) Write(b []byte) (int, error) {
	err := wsutil.WriteServerBinary(c.conn, b)
	n := 0
	if err == nil {
		n = len(b)
		c.stat.OnSent(n)
	}

	return n, err
}

// ConfigWS listener object for websocket server
type ConfigWS struct {
	CertFile  string
	KeyFile   string
	Path      string
	transport *Config
}

type ws struct {
	baseConfig
	httpServer
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
	} else if config.Path[0] != '/' {
		config.Path = "/" + config.Path
	}

	l.http = &http.Server{
		Addr:    ":" + config.transport.Port,
		Handler: l,
	}

	// initialize upgrader with custom callback for protocol validation
	// this callback simply returns "true" as protocol is prevalidated by ServeHTTP below
	l.up.Protocol = func(string) bool {
		return true
	}

	return l, nil
}

func (l *ws) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	proto := r.Header.Get("Sec-WebSocket-Protocol")
	if proto == "" {
		_, _ = w.Write([]byte("bad \"Sec-WebSocket-Protocol\""))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if !subProtocolRegexp.Match([]byte(proto)) {
		_, _ = w.Write([]byte("unsupported \"Sec-WebSocket-Protocol\""))
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}

	cn, _, _, err := l.up.Upgrade(r, w)
	if err != nil {
		l.log.Errorf("upgrade error: %s", err)
		return
	}

	l.onConnection.Add(1)
	go func() {
		defer l.onConnection.Done()
		l.handleConnection(l.newConn(cn, l.Metrics))
	}()
}

func (l *ws) newConn(cn net.Conn, stat metrics.Bytes) Conn {
	wr := newConn(cn, stat)

	c := &wsConn{
		conn: wr,
	}

	return c
}

// nolint:unused
func (l *ws) serve(w http.ResponseWriter, r *http.Request) {
	cn, _, _, err := gws.UpgradeHTTP(r, w)
	if err != nil {
		l.log.Errorf("upgrade error: %s", err)
		return
	}

	l.onConnection.Add(1)
	go func() {
		defer l.onConnection.Done()
		l.handleConnection(l.newConn(cn, l.Metrics))
	}()
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
