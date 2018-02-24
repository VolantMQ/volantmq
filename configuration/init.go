package configuration

import (
	"flag"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/zap"
)

type config struct {
	log      *zap.SugaredLogger
	humanLog *zap.SugaredLogger
	once     sync.Once
}

// Options global MQTT config
type Options struct {
	// LogWithTs either display timestamp messages on log or not
	LogWithTs bool
}

var cfg config

var configFile string

// WorkDir absolute path to service working directory
var WorkDir string

// PluginsDir absolute path to service plugins directory
var PluginsDir string

func init() {
	// initialize startup logger
	logCfg := zap.NewProductionConfig()

	logCfg.DisableStacktrace = true
	logCfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	logCfg.EncoderConfig.TimeKey = ""
	logCfg.EncoderConfig.LevelKey = ""
	logCfg.EncoderConfig.CallerKey = ""
	logCfg.Encoding = "console"
	log, _ := logCfg.Build()

	cfg.humanLog = log.Sugar()

	logCfg = zap.NewProductionConfig()

	logCfg.DisableStacktrace = true
	logCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logCfg.EncoderConfig.TimeKey = ""
	logCfg.Encoding = "console"
	log, _ = logCfg.Build()

	cfg.log = log.Sugar()

	WorkDir = "/var/lib/volantmq"
	PluginsDir = WorkDir + "/plugins"

	configFile, _ = os.LookupEnv("VOLANTMQ_CONFIG")

	if str, ok := os.LookupEnv("VOLANTMQ_WORK_DIR"); ok {
		WorkDir = str
	}

	if str, ok := os.LookupEnv("VOLANTMQ_PLUGINS_DIR"); ok {
		PluginsDir = str
	} else {
		PluginsDir = WorkDir + "/plugins"
	}

	flag.StringVar(&configFile, "config", configFile, "config file")
	flag.StringVar(&WorkDir, "work-dir", WorkDir, "service work directory")
	flag.StringVar(&PluginsDir, "plugins-dir", PluginsDir, "service plugins directory")

	var err error
	WorkDir, err = filepath.Abs(WorkDir)
	if err != nil {
		panic(err.Error())
	}

	PluginsDir, err = filepath.Abs(PluginsDir)
	if err != nil {
		panic(err.Error())
	}
}

// GetLogger return production logger
func GetLogger() *zap.SugaredLogger {
	return cfg.log
}

// GetHumanLogger return production logger
func GetHumanLogger() *zap.SugaredLogger {
	return cfg.humanLog
}
