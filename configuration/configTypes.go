package configuration

import (
	"crypto/tls"
	"io/ioutil"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/pkg/errors"
)

// PluginsConfig entry in system.plugins
type PluginsConfig struct {
	Enabled []string               `yaml:"enabled,omitempty"`
	Config  map[string]interface{} `yaml:"config,omitempty"`
}

// LogConfigBase base entry for all logger
type LogConfigBase struct {
	Timestamp *struct {
		Format string `yaml:"format" default:"2006-01-02T15:04:05Z07:00"`
	} `yaml:"timestamp,omitempty"`
	Level     string `yaml:"level"`
	Backtrace bool   `yaml:"backtrace"`
}

func (s *LogConfigBase) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type rawStuff LogConfigBase

	raw := rawStuff{
		Level:     "info",
		Backtrace: false,
	}

	if err := unmarshal(&raw); err != nil {
		return err
	}

	if raw.Timestamp != nil && raw.Timestamp.Format == "" {
		raw.Timestamp.Format = time.RFC3339
	}

	*s = LogConfigBase(raw)

	return nil
}

// ConsoleLogConfig entry in system.log.console
type ConsoleLogConfig struct {
	LogConfigBase `yaml:",inline"`
}

// SysLogConfig entry in system.log.syslog
type SysLogConfig struct {
	LogConfigBase `yaml:",inline"`
}

// FileLogConfig entry in system.log.file
type FileLogConfig struct {
	LogConfigBase `yaml:",inline"`
	File          string `yaml:"file,omitempty"`
	MaxSize       int    `yaml:"maxSize,omitempty"`
	MaxBackups    int    `yaml:"maxBackups,omitempty"`
	MaxAge        int    `yaml:"maxAge,omitempty"`
}

// LogConfig entry in system.log
type LogConfig struct {
	Console ConsoleLogConfig `yaml:"console"`
	SysLog  *SysLogConfig    `yaml:"syslog,omitempty"`
	File    *FileLogConfig   `yaml:"file,omitempty"`
}

// SystemConfig entry in system
type SystemConfig struct {
	Log  LogConfig `yaml:"log"`
	Http struct {
		DefaultPort string `yaml:"defaultPort"`
	} `yaml:"http"`
}

// TLSConfig used by SecurityConfig or ssl/ws listeners
type TLSConfig struct {
	Cert string `yaml:"cert,omitempty"`
	Key  string `yaml:"key,omitempty"`
}

// SecurityConfig system security config
type SecurityConfig struct {
	TLS TLSConfig `yaml:"tls,omitempty"`
}

type AuthConfig struct {
	Anonymous bool     `yaml:"anonymous,omitempty" default:"false"`
	Order     []string `yaml:"order"`
}

// PortConfig configuration of tcp/ssl/ws(s) listeners
type PortConfig struct {
	Host string     `yaml:"host,omitempty"`
	Auth AuthConfig `yaml:"auth"`
	TLS  TLSConfig  `yaml:"tls,omitempty"`
	Path string     `yaml:"path,omitempty"`
}

// MqttConfig server config
type MqttConfig struct {
	Version []string `yaml:"version,omitempty"`
	Systree struct {
		Enabled        bool `yaml:"enabled,omitempty"`
		UpdateInterval int  `yaml:"updateInterval,omitempty"`
	} `yaml:"systree,omitempty"`
	KeepAlive struct {
		Period int  `yaml:"period,omitempty"`
		Force  bool `yaml:"force,omitempty"`
	} `yaml:"keepAlive,omitempty"`
	Options struct {
		ConnectTimeout  int           `yaml:"connectTimeout,omitempty"`
		SessionDups     bool          `yaml:"sessionDups,omitempty"`
		RetainAvailable bool          `yaml:"retainAvailable,omitempty"`
		SubsOverlap     bool          `yaml:"subsOverlap,omitempty"`
		SubsID          bool          `yaml:"subsId,omitempty"`
		SubsShared      bool          `yaml:"subsShared,omitempty"`
		SubsWildcard    bool          `yaml:"subsWildcard,omitempty"`
		ReceiveMax      uint16        `yaml:"receiveMax,omitempty"`
		MaxPacketSize   uint32        `yaml:"maxPacketSize,omitempty"`
		MaxTopicAlias   uint16        `yaml:"maxTopicAlias,omitempty"`
		MaxQoS          mqttp.QosType `yaml:"maxQoS,omitempty"`
		OfflineQoS0     bool          `yaml:"offlineQoS0,omitempty"`
	}
}

// ListenersConfig
type ListenersConfig struct {
	DefaultAddr string                           `yaml:"defaultAddr,omitempty"`
	MQTT        map[string]map[string]PortConfig `yaml:"mqtt,omitempty"`
}

// Config system-wide config
type Config struct {
	System    SystemConfig    `yaml:"system"`
	Plugins   PluginsConfig   `yaml:"plugins,omitempty"`
	Mqtt      MqttConfig      `yaml:"mqtt,omitempty"`
	Listeners ListenersConfig `yaml:"listeners,omitempty"`
	Security  SecurityConfig  `yaml:"security,omitempty"`
	Auth      AuthConfig      `yaml:"auth,omitempty"`
}

func (t *TLSConfig) Validate() (tls.Certificate, error) {
	if len(t.Cert) == 0 {
		return tls.Certificate{}, errors.New("empty certificate name")
	}

	if len(t.Key) == 0 {
		return tls.Certificate{}, errors.New("empty key name")
	}

	certPEMBlock, err := ioutil.ReadFile(t.Cert)
	if err != nil {
		return tls.Certificate{}, errors.New("tls: read certificate: " + t.Cert + " " + err.Error())
	}

	keyPEMBlock, err := ioutil.ReadFile(t.Key)
	if err != nil {
		return tls.Certificate{}, errors.New("tls: read key: " + t.Key + " " + err.Error())
	}

	return tls.X509KeyPair(certPEMBlock, keyPEMBlock)
}

func (t *TLSConfig) LoadConfig() (*tls.Config, error) {
	certs, err := t.Validate()
	if err != nil {
		return nil, err
	}

	c := &tls.Config{}

	c.Certificates = append(c.Certificates, certs)

	return c, nil
}
