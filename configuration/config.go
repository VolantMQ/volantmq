package configuration

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/VolantMQ/vlapi/plugin"
	"gopkg.in/yaml.v2"
)

// PluginState status
type PluginState struct {
	Plugin vlplugin.Plugin
	Errors []error
}

// DefaultConfig Load minimum working configuration to allow
// server start without user provided one
func DefaultConfig() *Config {
	c := Config{}
	if err := yaml.Unmarshal(defaultConfig, &c); err != nil {
		panic(err.Error())
	}

	return &c
}

// ReadConfig read service configuration
func ReadConfig() *Config {
	log := GetHumanLogger()
	log.Info("loading config")

	flag.Parse()

	c := DefaultConfig()

	if len(configFile) == 0 {
		log.Info("No config file provided\nuse --config option or VOLANTMQ_CONFIG environment variable to provide own")
		log.Info("default config: \n", string(defaultConfig))
	} else {
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			log.Error("config not found", "file", configFile)
			return nil
		}

		data, err := ioutil.ReadFile(configFile)
		if err != nil {
			panic(err.Error())
		}

		if err = yaml.Unmarshal(data, c); err != nil {
			panic(err.Error())
		}
	}

	return c
}
