package configuration

import (
	"sync"

	"go.uber.org/zap"
)

type config struct {
	log  *zap.Logger
	once sync.Once
}

// Options global MQTT config
type Options struct {
	// LogWithTs either display timestamp messages on log or not
	LogWithTs bool
}

var cfg config

func init() {
	logCfg := zap.NewProductionConfig()

	logCfg.DisableStacktrace = true
	logCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)

	log, _ := logCfg.Build()

	cfg.log = log.Named("mqtt")
}

// Init global MQTT config with given options
// if not being called default set by init() is used
func Init(ops Options) {
	cfg.once.Do(func() {
		logCfg := zap.NewProductionConfig()
		logCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)

		logCfg.DisableStacktrace = true

		if !ops.LogWithTs {
			logCfg.EncoderConfig.TimeKey = ""
		}

		log, _ := logCfg.Build()
		cfg.log = log.Named("mqtt")
	})
}

// GetLogger return production logger
func GetLogger() *zap.Logger {
	return cfg.log
}
