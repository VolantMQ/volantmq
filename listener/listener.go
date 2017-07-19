package listener

import (
	"go.uber.org/zap"

	"sync"

	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/types"
)

type InternalConfig struct {
	HandleConnection func(c types.Conn, auth *auth.Manager)
	Log              *zap.Logger
	Metric           systree.BytesMetric
}

type baseConfig struct {
	InternalConfig

	quit chan struct{}
	auth *auth.Manager
	log  *zap.Logger

	onStop       func(int)
	onConnection sync.WaitGroup
	once         struct {
		start sync.Once
		stop  sync.Once
	}
	lPort int
}

// Config listener
type Config interface{}

type Provider interface {
	Protocol() string
	Serve() error
	Close() error
	Port() int
}

func (l *baseConfig) Port() int {
	return l.lPort
}
