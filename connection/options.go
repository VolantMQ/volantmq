package connection

import (
	"errors"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/vlauth"
	"github.com/VolantMQ/vlapi/vlpersistence"

	"github.com/VolantMQ/volantmq/metrics"
	"github.com/VolantMQ/volantmq/transport"
)

// OnAuthCb ...
type OnAuthCb func(string, *AuthParams) (mqttp.IFace, error)

// Option callback for connection option
type Option func(*impl) error

// SetOptions set connection options
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
		return wrOfflineQoS0(val)(t.tx)
	}
}

// KeepAlive keep alive period
func KeepAlive(val int) Option {
	return func(t *impl) error {
		vl := time.Duration(val+(val/2)) * time.Second
		return rdKeepAlive(vl)(t.rx)
	}
}

func Metric(val metrics.Packets) Option {
	return func(t *impl) error {
		t.metric = val
		if err := wrMetric(val)(t.tx); err != nil {
			return err
		}
		return rdMetric(val)(t.rx)
	}
}

func MaxRxPacketSize(val uint32) Option {
	return func(t *impl) error {
		return rdMaxPacketSize(val)(t.rx)
	}
}

func MaxTxPacketSize(val uint32) Option {
	return func(t *impl) error {
		return wrMaxPacketSize(val)(t.tx)
	}
}

func TxQuota(val int32) Option {
	return func(t *impl) error {
		return wrQuota(val)(t.tx)
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
		return wrTopicAliasMax(val)(t.tx)
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
		if err := wrConn(val)(t.tx); err != nil {
			return err
		}
		return rdConn(val)(t.rx)
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

func Persistence(val vlpersistence.Packets) Option {
	return func(t *impl) error {
		return wrPersistence(val)(t.tx)
	}
}

func Permissions(val vlauth.Permissions) Option {
	return func(t *impl) error {
		t.permissions = val
		return nil
	}
}
