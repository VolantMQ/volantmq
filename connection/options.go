package connection

import (
	"errors"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/plugin/persistence"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/transport"
)

type OnAuthCb func(string, *AuthParams) (mqttp.IFace, error)

type Option func(*impl) error

func (s *impl) SetOptions(opts ...Option) error {
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return err
		}
	}

	return nil
}

// OfflineQoS0 if true QoS0 messages will be persisted when session is offline and durable
func OfflineQoS0(val bool) Option {
	return func(t *impl) error {
		wrOfflineQoS0(val)(t.tx)
		return nil
	}
}

// KeepAlive keep alive period
func KeepAlive(val int) Option {
	return func(t *impl) error {
		vl := time.Duration(val+(val/2)) * time.Second
		rdKeepAlive(vl)(t.rx)
		return nil
	}
}

func Metric(val systree.PacketsMetric) Option {
	return func(t *impl) error {
		t.metric = val
		wrMetric(val)(t.tx)
		rdMetric(val)(t.rx)
		return nil
	}
}

func MaxRxPacketSize(val uint32) Option {
	return func(t *impl) error {
		rdMaxPacketSize(val)(t.rx)
		return nil
	}
}

func MaxTxPacketSize(val uint32) Option {
	return func(t *impl) error {
		wrMaxPacketSize(val)(t.tx)
		return nil
	}
}

func TxQuota(val int32) Option {
	return func(t *impl) error {
		wrQuota(val)(t.tx)
		return nil
	}
}

func RxQuota(val int32) Option {
	return func(t *impl) error {
		t.rxQuota = val
		return nil
	}
}

func MaxTxTopicAlias(val uint16) Option {
	return func(t *impl) error {
		wrTopicAliasMax(val)(t.tx)
		return nil
	}
}

func MaxRxTopicAlias(val uint16) Option {
	return func(t *impl) error {
		t.maxRxTopicAlias = val
		return nil
	}
}

func RetainAvailable(val bool) Option {
	return func(t *impl) error {
		t.retainAvailable = val
		return nil
	}
}

func OnAuth(val OnAuthCb) Option {
	return func(t *impl) error {
		t.signalAuth = val
		return nil
	}
}

func NetConn(val transport.Conn) Option {
	return func(t *impl) error {
		if t.conn != nil {
			return errors.New("already set")
		}

		t.conn = val
		wrConn(val)(t.tx)
		rdConn(val)(t.rx)
		return nil
	}
}

func AttachSession(val SessionCallbacks) Option {
	return func(t *impl) error {
		if t.SessionCallbacks != nil {
			return errors.New("already set")
		}
		t.SessionCallbacks = val
		return nil
	}
}

func Persistence(val persistence.Packets) Option {
	return func(t *impl) error {
		return wrPersistence(val)(t.tx)
	}
}
