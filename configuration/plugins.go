package configuration

import (
	"plugin"

	vlPlugin "github.com/VolantMQ/plugin"
)

// PluginState status
type PluginState struct {
	Plugin vlPlugin.Plugin
	Errors []error
}

// LoadPlugins load plugins from path
func LoadPlugins(path string, list []string) map[string]PluginState {
	log := GetHumanLogger()
	log.Info("loading plugins")

	if len(path) == 0 {
		log.Info("\tno plugins:", "empty path")
		return nil
	}

	if len(list) == 0 {
		log.Info("\tno plugins:", "empty list")
		return nil
	}

	plugins := make(map[string]PluginState)

	if path[len(path)-1] != '/' {
		path += "/"
	}

	for _, p := range list {
		pl := PluginState{}

		where := path + p + ".so"
		plEntry, err := plugin.Open(where)
		if err != nil {
			pl.Errors = append(pl.Errors, err)
		} else {
			var sym plugin.Symbol

			if sym, err = plEntry.Lookup("Plugin"); err != nil {
				pl.Errors = append(pl.Errors, err)
			} else {
				pl.Plugin = sym.(vlPlugin.Plugin)
			}
		}
		plugins[p] = pl
	}

	return plugins
}
