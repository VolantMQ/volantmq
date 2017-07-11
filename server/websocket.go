package server

import (
	"net/http"

	"errors"
	"strconv"

	"crypto/tls"

	"github.com/gorilla/websocket"
	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
	//"github.com/gorilla/mux"
)

// ListenerWS listener object for websocket server
type ListenerWS struct {
	ListenerBase

	up  websocket.Upgrader
	log types.LogInterface
}

func (l *ListenerWS) serveWs(w http.ResponseWriter, r *http.Request) {
	cn, err := l.up.Upgrade(w, r, nil)
	if err != nil {
		l.log.Prod.Error("Couldn't upgrade WebSocket connection", zap.Error(err))
		return
	}

	l.inner.wgConnections.Add(1)
	defer l.inner.wgConnections.Done()
	if conn, err := types.NewConnWs(cn, l.inner.sysTree.Metric().Bytes()); err != nil {
		l.log.Prod.Error("Couldn't create connection interface", zap.Error(err))
	} else {
		if err = l.handleConnection(conn); err != nil {
			l.log.Prod.Error("Couldn't handle connection", zap.Error(err))
		}
	}

	//go func(cn *websocket.Conn) {

	//}(conn)
}

func (l *ListenerWS) start() error {
	select {
	case <-l.inner.quit:
		return nil
	default:
	}

	defer l.inner.lock.Unlock()
	l.inner.lock.Lock()

	l.up.Subprotocols = make([]string, 3)
	l.up.Subprotocols[0] = "mqtt"
	l.up.Subprotocols[1] = "mqttv3.1"
	l.up.Subprotocols[2] = "mqttv3.1.1"

	var err error

	isTLS := false

	if l.CertFile != "" && l.KeyFile != "" {
		certificates := make([]tls.Certificate, 1)

		if certificates[0], err = tls.LoadX509KeyPair(l.CertFile, l.KeyFile); err != nil {
			return err
		}
		isTLS = true
	}

	//mux := http.NewServeMux()
	http.HandleFunc("/mqtt", l.serveWs)

	if _, ok := l.inner.listeners.list[l.Port]; !ok {
		l.inner.listeners.list[l.Port] = l
		l.inner.listeners.wg.Add(1)

		go func() {
			defer l.inner.listeners.wg.Done()

			var statusAddr string

			if isTLS {
				statusAddr = "wss://:" + strconv.Itoa(l.Port)
			} else {
				statusAddr = "ws://:" + strconv.Itoa(l.Port)
			}
			if l.inner.config.ListenerStatus != nil {
				l.inner.config.ListenerStatus(statusAddr, true)
			}

			if isTLS {
				err = http.ListenAndServeTLS(":"+strconv.Itoa(l.Port), l.CertFile, l.KeyFile, nil)
			} else {
				err = http.ListenAndServe(":"+strconv.Itoa(l.Port), nil)
			}

			if l.inner.config.ListenerStatus != nil {
				l.inner.config.ListenerStatus(statusAddr, false)
			}
		}()
	} else {
		err = errors.New("Listener already exists")
	}

	return err
}

func (l *ListenerWS) close() error {
	return nil
}

func (l *ListenerWS) listenerProtocol() string {
	return "tcp"
}
