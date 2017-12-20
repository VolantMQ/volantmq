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
	// LogLevel options are: debug,info,warn,error,panic,fatal
	LogLevel string
	// LogEnableTrace whether capture the trace info in error and above level logs.
	LogEnableTrace bool
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

		logCfg.DisableStacktrace = !ops.LogEnableTrace

		if !ops.LogWithTs {
			logCfg.EncoderConfig.TimeKey = ""
		}

		switch ops.LogLevel {
		case "debug":
			logCfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		case "info":
			logCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
		case "warn":
			logCfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
		case "error":
			logCfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
		case "panic":
			logCfg.Level = zap.NewAtomicLevelAt(zap.PanicLevel)
		case "fatal":
			logCfg.Level = zap.NewAtomicLevelAt(zap.FatalLevel)
		default:
			logCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)

		}
		log, _ := logCfg.Build()
		cfg.log = log.Named("mqtt")
	})
}

// GetLogger return production logger
func GetLogger() *zap.Logger {
	return cfg.log
}
