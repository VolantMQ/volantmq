package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	vlAuth "github.com/VolantMQ/auth"
	"github.com/VolantMQ/persistence"
	"github.com/VolantMQ/plugin"
	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/server"
	"github.com/VolantMQ/volantmq/transport"
	"go.uber.org/zap"
)

type pluginTypes map[string]map[string]plugin.Plugin

func loadMqttListeners(defaultAuth *auth.Manager, lCfg *configuration.ListenersConfig) ([]interface{}, error) {
	var listeners []interface{}

	for name, ls := range lCfg.MQTT {
		for port, cfg := range ls {
			transportAuth := defaultAuth

			if len(cfg.Auth) > 0 {
				transportAuth, _ = auth.NewManager(cfg.Auth)
			}

			host := lCfg.DefaultAddr
			if len(cfg.Host) > 0 {
				host = cfg.Host
			}

			tCfg := &transport.Config{
				Host:        host,
				Port:        port,
				AuthManager: transportAuth,
			}

			switch name {
			case "tcp", "ssl":
				tcpConfig := transport.NewConfigTCP(tCfg)

				if name == "ssl" {
					tlsConfig, err := cfg.TLS.LoadConfig()
					if err != nil {
						logger.Error("\tlisteners.mqtt.ssl: ", err.Error())
						return nil, err
					}
					tcpConfig.TLS = tlsConfig
				}

				listeners = append(listeners, tcpConfig)
			case "ws", "wss":
				configWs := transport.NewConfigWS(tCfg)
				configWs.Path = cfg.Path

				if name == "wss" {
					if _, err := cfg.TLS.Validate(); err != nil {
						logger.Error("\tlisteners.mqtt.wss: ", err.Error())
						return nil, err
					}
					configWs.CertFile = cfg.TLS.Cert
					configWs.KeyFile = cfg.TLS.Key
				}

				listeners = append(listeners, configWs)
			default:
				logger.Fatal("\tunknown mqtt listener type", zap.String("type", name))
				return nil, errors.New("")
			}
		}
	}

	return listeners, nil
}

func loadListeners(defaultAuth *auth.Manager, lst *configuration.ListenersConfig) (map[string][]interface{}, error) {
	logger.Info("configuring listeners")

	listeners := make(map[string][]interface{})

	if len(lst.MQTT) > 0 {
		ls, err := loadMqttListeners(defaultAuth, lst)
		if err != nil {
			return nil, err
		}

		listeners["mqtt"] = ls
	}

	return listeners, nil
}

func loadAuth(cfg *configuration.Config, plTypes pluginTypes) (*auth.Manager, error) {
	logger.Info("configuring auth")

	a, ok := cfg.Plugins.Config["auth"]
	if !ok {
		logger.Fatalf("\tno auth config found at plugins.config.auth")
		return nil, errors.New("")
	}

	if len(cfg.Auth.DefaultOrder) == 0 {
		logger.Fatalf("\tdefault auth order should not be empty auth.defaultOrder")
		return nil, errors.New("")
	}

	for nm, i := range a.(map[interface{}]interface{}) {
		var provider vlAuth.Provider

		pv := i.(map[interface{}]interface{})
		switch pv["type"] {
		case "simpleAuth":
			ia := internalAuth{
				creds: make(map[string]string),
			}

			provider = ia
			list := pv["users"].(map[interface{}]interface{})
			for u, p := range list {
				ia.creds[u.(string)] = p.(string)
			}
		}

		if err := auth.Register(nm.(string), provider); err != nil {
			logger.Error("\tauth provider", zap.String("name", nm.(string)), zap.String("type", pv["type"].(string)), zap.Error(err))

			return nil, err
		}
	}

	def, err := auth.NewManager(cfg.Auth.DefaultOrder)

	if err != nil {
		logger.Error("\tcreating default auth: %s", err.Error())
		return nil, err
	}

	logger.Info("\tdefault order: ", cfg.Auth.DefaultOrder)

	return def, nil
}

func loadPlugins(cfg *configuration.PluginsConfig) (pluginTypes, error) {
	plugins := configuration.LoadPlugins(configuration.PluginsDir, cfg.Enabled)

	plTypes := make(pluginTypes)

	if len(plugins) > 0 {
		for name, pl := range plugins {
			if len(pl.Errors) > 0 {
				logger.Info("\t", name, pl.Errors)
			} else if pl.Plugin == nil {
				logger.Info("\t", name, "Not Found")
			} else {
				logger.Info("\t", name, ":",
					"\n\t\tPlugins API Version: ", pl.Plugin.Info().APIVersion(),
					"\n\t\t     Plugin Version: ", pl.Plugin.Info().Version(),
					"\n\t\t               Type: ", pl.Plugin.Info().Type(),
					"\n\t\t        Description: ", pl.Plugin.Info().Desc(),
					"\n\t\t               Name: ", pl.Plugin.Info().Name())
				if _, ok := plTypes[pl.Plugin.Info().Type()]; !ok {
					plTypes[pl.Plugin.Info().Type()] = make(map[string]plugin.Plugin)
				}

				plTypes[pl.Plugin.Info().Type()][pl.Plugin.Info().Name()] = pl.Plugin
			}
		}
	}

	return plTypes, nil
}

func loadPersistence(cfg interface{}, plTypes pluginTypes) (persistence.Provider, error) {
	persist := persistence.Default()

	logger.Info("loading persistence")
	if cfg == nil {
		logger.Warn("\tno persistence backend provided\n\tusing in-memory. Data will be lost on shutdown")
	} else {
		pProvidersConfig := cfg.(map[interface{}]interface{})
		if len(pProvidersConfig) > 1 {
			logger.Warn("\tplugins.config.persistence: multiple persistence providers not supported")
			return nil, errors.New("")
		}

		for nm, pCfg := range pProvidersConfig {
			name := nm.(string)
			if pl, kk := plTypes["persistence"][name]; !kk {
				logger.Fatalf("\tplugins.config.persistence: plugin type [%s] not found", name)
				return nil, errors.New("")
			} else {
				if plObject, err := pl.Load(pCfg); err != nil {
					logger.Fatalf("\tplugin [%s] acquire failed: %s", name, err.Error())
					return nil, errors.New("")
				} else {
					persist = plObject.(persistence.Provider)
					logger.Infof("\tusing persistence provider [%s]", name)
				}
			}

			break
		}
	}

	return persist, nil
}

var logger *zap.SugaredLogger

func main() {
	defer func() {
		fmt.Println("service stopped")
	}()

	logger = configuration.GetHumanLogger()
	logger.Info("starting service...")
	logger.Info("working directory: ", configuration.WorkDir)
	logger.Info("plugins directory: ", configuration.PluginsDir)

	config := configuration.ReadConfig()
	if config == nil {
		return
	}

	var persist persistence.Provider
	var plTypes pluginTypes
	var defaultAuth *auth.Manager

	var err error

	if plTypes, err = loadPlugins(&config.Plugins); err != nil {
		return
	}

	if persist, err = loadPersistence(config.Plugins.Config["persistence"], plTypes); err != nil {
		return
	}

	if defaultAuth, err = loadAuth(config, plTypes); err != nil {
		return
	}

	var listeners map[string][]interface{}

	if listeners, err = loadListeners(defaultAuth, &config.Listeners); err != nil {
		return
	}

	if len(listeners["mqtt"]) == 0 {
		logger.Error("no mqtt listeners")
		return
	}

	var srv server.Server

	listenerStatus := func(id string, status string) {
		logger.Info("listener state: ", "id: ", id, " status: ", status)
	}

	serverConfig := server.Config{
		MQTT:            config.Mqtt,
		TransportStatus: listenerStatus,
		Persistence:     persist,
	}

	if srv, err = server.NewServer(serverConfig); err != nil {
		logger.Error("server create", zap.Error(err))
		return
	}

	logger.Info("MQTT server created")
	logger.Info("MQTT starting listeners")
	for _, l := range listeners["mqtt"] {
		if err = srv.ListenAndServe(l); err != nil {
			logger.Fatal("listen and serve", zap.Error(err))
			break
		}
	}

	if err == nil {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		sig := <-ch
		logger.Info("service received signal: ", sig.String())
	}

	if err = srv.Close(); err != nil {
		logger.Error("shutdown server", zap.Error(err))
	}
}
