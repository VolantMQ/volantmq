package connection

import (
	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/plugin/persistence"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/transport"
	"go.uber.org/zap"
)

func (s *writer) setOptions(opts ...writerOption) error {
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return err
		}
	}

	return nil
}

func wrID(val string) writerOption {
	return func(t *writer) error {
		t.id = val
		return nil
	}
}

func wrOnConnClose(val signalConnectionClose) writerOption {
	return func(t *writer) error {
		t.onConnectionClose = val
		return nil
	}
}

func wrConn(val transport.Conn) writerOption {
	return func(t *writer) error {
		t.conn = val
		return nil
	}
}

func wrMetric(val systree.PacketsMetric) writerOption {
	return func(t *writer) error {
		t.metric = val
		return nil
	}
}

func wrPersistence(val persistence.Packets) writerOption {
	return func(t *writer) error {
		t.persist = val
		return nil
	}
}

func wrMaxPacketSize(val uint32) writerOption {
	return func(t *writer) error {
		t.packetMaxSize = val
		return nil
	}
}

func wrQuota(val int32) writerOption {
	return func(t *writer) error {
		t.flow.quota = val
		return nil
	}
}

func wrTopicAliasMax(val uint16) writerOption {
	return func(t *writer) error {
		t.topicAliasMax = val
		return nil
	}
}

func wrVersion(val mqttp.ProtocolVersion) writerOption {
	return func(t *writer) error {
		t.version = val
		return nil
	}
}

func wrOfflineQoS0(val bool) writerOption {
	return func(t *writer) error {
		t.offlineQoS0 = val
		return nil
	}
}

func wrLog(val *zap.SugaredLogger) writerOption {
	return func(t *writer) error {
		t.log = val
		return nil
	}
}
