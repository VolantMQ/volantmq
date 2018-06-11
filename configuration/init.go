package configuration

import (
	"flag"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type config struct {
	//log      *zap.SugaredLogger
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
	logCfg.EncoderConfig.LevelKey = ""
	logCfg.EncoderConfig.CallerKey = ""
	logCfg.Encoding = "console"
	logCfg.EncoderConfig.EncodeTime = func(t time.Time, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString(t.Format(time.RFC3339))
	}

	log, _ := logCfg.Build()

	cfg.humanLog = log.Sugar()

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
	//return cfg.log
	return cfg.humanLog
}

// GetHumanLogger return production logger
func GetHumanLogger() *zap.SugaredLogger {
	return cfg.humanLog
}

var configTimeFormatMap = map[string]string{
	"ANSIC":       time.ANSIC,
	"UNIX":        time.UnixDate,
	"RubyDate":    time.RubyDate,
	"RFC822":      time.RFC822,
	"RFC822Z":     time.RFC822Z,
	"RFC850":      time.RFC850,
	"RFC1123":     time.RFC1123,
	"RFC1123Z":    time.RFC1123Z,
	"RFC3339":     time.RFC3339,
	"RFC3339Nano": time.RFC3339Nano,
}

func ConfigureLoggers(c *LogConfig) error {
	// initialize startup logger
	//logCfg := zap.NewProductionConfig()

	logCfg := zap.NewDevelopmentEncoderConfig()

	var level zapcore.Level
	if err := level.UnmarshalText([]byte(c.Console.Level)); err != nil {
		return err
	}

	if c.Console.Timestamp != nil {
		if f, ok := configTimeFormatMap[c.Console.Timestamp.Format]; !ok {
			GetLogger().Warn("unsupported time format supplied by config. using RFCC3339")
			c.Console.Timestamp.Format = time.RFC3339
		} else {
			c.Console.Timestamp.Format = f
		}
		logCfg.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format(c.Console.Timestamp.Format))
		}
	} else {
		logCfg.EncodeTime = nil
	}

	logCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logCfg.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(logCfg)

	//logger, _ = logCfg.Build()
	// High-priority output should also go to standard error, and low-priority
	// output should also go to standard out.
	consoleDebugging := zapcore.Lock(os.Stdout)
	consoleErrors := zapcore.Lock(os.Stderr)

	// First, define our level-handling logic.
	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})
	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.ErrorLevel
	})

	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, consoleErrors, highPriority),
		zapcore.NewCore(consoleEncoder, consoleDebugging, lowPriority))

	cfg.humanLog = zap.New(core).Sugar()

	cfg.humanLog.Sync()

	return nil
}
