// +build woPlugins

package configuration

// LoadPlugins load plugins from path
func LoadPlugins(string, []string) map[string]PluginState {
	log := GetHumanLogger()
	log.Info("plugins are not supported for this build")

	plugins := make(map[string]PluginState)

	return plugins
}
