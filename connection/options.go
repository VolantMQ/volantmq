package connection

import (
	"errors"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/transport"
)

type OnAuthCb func(string, *AuthParams) (mqttp.Provider, error)

type Option func(*impl) error

func (s *impl) SetOptions(opts ...Option) error {
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return err
		}
	}

	return nil
}
func OfflineQoS0(val bool) Option {
	return func(t *impl) error {
		t.offlineQoS0 = val
		return nil
	}
}

func KeepAlive(val int) Option {
	return func(t *impl) error {
		vl := time.Duration(val) * time.Second
		t.keepAlive = vl + (vl / 2)
		return nil
	}
}

func Metric(val systree.PacketsMetric) Option {
	return func(t *impl) error {
		t.metric = val
		return nil
	}
}

func MaxRxPacketSize(val uint32) Option {
	return func(t *impl) error {
		t.maxRxPacketSize = val
		return nil
	}
}

func MaxTxPacketSize(val uint32) Option {
	return func(t *impl) error {
		t.maxTxPacketSize = val
		return nil
	}
}

func TxQuota(val int32) Option {
	return func(t *impl) error {
		t.txQuota = val
		//t.pubOut.quota = val
		return nil
	}
}

func RxQuota(val int32) Option {
	return func(t *impl) error {
		t.rxQuota = val
		//t.pubIn.quota = val
		return nil
	}
}

func MaxTxTopicAlias(val uint16) Option {
	return func(t *impl) error {
		t.maxTxTopicAlias = val
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
