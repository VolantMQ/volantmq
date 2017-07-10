package surgemq

import (
	"sync"

	"go.uber.org/zap"
)

type config struct {
	log struct {
		Prod *zap.Logger
		Dev  *zap.Logger
	}

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
	logDebugCfg := zap.NewDevelopmentConfig()

	cfg.log.Prod, _ = logCfg.Build()
	cfg.log.Dev, _ = logDebugCfg.Build()
	cfg.log.Prod.Named("mqtt")
	cfg.log.Dev.Named("mqtt")
}

// Init global MQTT config with given options
// if not being called default set by init() is used
func Init(ops Options) {
	cfg.once.Do(func() {
		logCfg := zap.NewProductionConfig()
		logDebugCfg := zap.NewDevelopmentConfig()

		if !ops.LogWithTs {
			logCfg.EncoderConfig.TimeKey = ""
			logDebugCfg.EncoderConfig.TimeKey = ""
		}

		log, _ := logCfg.Build()
		dLog, _ := logDebugCfg.Build()
		cfg.log.Prod = log.Named("mqtt")
		cfg.log.Dev = dLog.Named("mqtt")
	})
}

// GetProdLogger return production logger
func GetProdLogger() *zap.Logger {
	return cfg.log.Prod
}

// GetDevLogger return development logger
func GetDevLogger() *zap.Logger {
	return cfg.log.Prod
}
