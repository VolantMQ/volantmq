package connection

import (
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/transport"
	"go.uber.org/zap"
)

func (s *reader) setOptions(opts ...readerOption) error {
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return err
		}
	}

	return nil
}

func rdOnConnClose(val signalConnectionClose) readerOption {
	return func(t *reader) error {
		t.onConnectionClose = val
		return nil
	}
}

func rdProcessIncoming(val signalIncoming) readerOption {
	return func(t *reader) error {
		t.processIncoming = val
		return nil
	}
}

func rdConn(val transport.Conn) readerOption {
	return func(t *reader) error {
		t.conn = val
		return nil
	}
}

func rdConnect(val chan interface{}) readerOption {
	return func(t *reader) error {
		t.connect = val
		return nil
	}
}

func rdMetric(val systree.PacketsMetric) readerOption {
	return func(t *reader) error {
		t.metric = val
		return nil
	}
}

func rdMaxPacketSize(val uint32) readerOption {
	return func(t *reader) error {
		t.packetMaxSize = val
		return nil
	}
}

func rdKeepAlive(val time.Duration) readerOption {
	return func(t *reader) error {
		t.keepAlive = val
		return nil
	}
}

func rdVersion(val mqttp.ProtocolVersion) readerOption {
	return func(t *reader) error {
		t.version = val
		return nil
	}
}

func rdLog(val *zap.SugaredLogger) readerOption {
	return func(t *reader) error {
		t.log = val
		return nil
	}
}
