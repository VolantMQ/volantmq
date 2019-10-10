// +build !woPlugins

package configuration

import (
	"path/filepath"
	"plugin"

	"github.com/VolantMQ/vlapi/vlplugin"
)

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

	for _, p := range list {
		pl := PluginState{}

		where := filepath.Join(path, p+".so")
		plEntry, err := plugin.Open(where)
		if err != nil {
			pl.Errors = append(pl.Errors, err)
		} else {
			var sym plugin.Symbol

			if sym, err = plEntry.Lookup("Plugin"); err != nil {
				pl.Errors = append(pl.Errors, err)
			} else {
				pl.Plugin = sym.(vlplugin.Plugin)
			}
		}
		plugins[p] = pl
	}

	return plugins
}
