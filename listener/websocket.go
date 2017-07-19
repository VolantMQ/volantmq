package listener

import (
	"net/http"

	"strconv"

	"crypto/tls"

	"context"
	"time"

	"github.com/gorilla/websocket"
	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
)

type httpServer struct {
	mux  *http.ServeMux
	http *http.Server
}

// ConfigWS listener object for websocket server
type ConfigWS struct {
	InternalConfig

	// Port
	Port int

	// CertFile
	CertFile string

	// KeyFile
	KeyFile string

	// AuthManager
	AuthManager *auth.Manager

	// Path
	Path string

	// SubProtocols
	SubProtocols []string
}

type ws struct {
	baseConfig

	certFile string
	keyFile  string
	up       websocket.Upgrader
	s        httpServer
}

func NewWS(config *ConfigWS) (Provider, error) {
	l := &ws{
		certFile: config.CertFile,
		keyFile:  config.KeyFile,
	}

	l.InternalConfig = config.InternalConfig

	l.quit = make(chan struct{})
	l.auth = config.AuthManager
	l.lPort = config.Port

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
		Addr:    ":" + strconv.Itoa(config.Port),
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
		if conn, err := types.NewConnWs(cn, l.Metric); err != nil {
			l.log.Error("Couldn't create connection interface", zap.Error(err))
		} else {
			l.HandleConnection(conn, l.auth)
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

	l.once.stop.Do(func() {
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
