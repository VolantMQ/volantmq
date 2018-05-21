package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/VolantMQ/persistence"
	"github.com/VolantMQ/vlauth"
	"github.com/VolantMQ/vlplugin"
	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/server"
	"github.com/VolantMQ/volantmq/transport"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type pluginType map[string]vlplugin.Plugin

type pluginTypes map[string]pluginType

func loadMqttListeners(defaultAuth *auth.Manager, lCfg *configuration.ListenersConfig) ([]interface{}, error) {
	var listeners []interface{}

	for name, ls := range lCfg.MQTT {
		for port, cfg := range ls {
			transportAuth := defaultAuth

			if len(cfg.Auth.Order) > 0 {
				var err error
				if transportAuth, err = auth.NewManager(cfg.Auth.Order, cfg.Auth.Anonymous); err != nil {
					return nil, err
				}
			}

			host := lCfg.DefaultAddr
			if len(cfg.Host) > 0 {
				host = cfg.Host
			}

			logger.Infof("host: %s", host)
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

func configureSimpleAuth(cfg interface{}) (vlauth.Iface, error) {
	sAuth := newSimpleAuth()
	authConfig := cfg.(map[interface{}]interface{})

	if list, kk := authConfig["users"].(map[interface{}]interface{}); kk {
		for u, p := range list {
			sAuth.addUser(u.(string), p.(string))
		}
	}

	if uFile, kk := authConfig["usersFile"].(string); kk {
		if fl, err := ioutil.ReadFile(uFile); err != nil {
			logger.Error("\treading simpleAuth users file: ", err.Error())
		} else {
			users := make(map[string]string)
			if err = yaml.Unmarshal(fl, &users); err != nil {
				logger.Error("\tdecoding simpleAuth users file: ", err.Error())
			} else {
				for u, p := range users {
					sAuth.addUser(u, p)
				}
			}
		}
	}

	if len(sAuth.creds) == 0 {
		logger.Warn("\tsimpleAuth config without users. setting default to guest:guest")
		sAuth.addUser("guest", string(sha256.New().Sum([]byte("guest"))))
	}

	return sAuth, nil
}

func loadAuth(cfg *configuration.Config, plTypes pluginTypes) (*auth.Manager, error) {
	logger.Info("configuring auth")

	a, ok := cfg.Plugins.Config["auth"]
	if !ok {
		logger.Fatalf("\tno auth config found at plugins.config.auth")
		return nil, errors.New("")
	}

	if len(cfg.Auth.Order) == 0 {
		logger.Fatalf("\tdefault auth order should not be empty auth.Order")
		return nil, errors.New("")
	}

	for idx, cfgEntry := range a.([]interface{}) {
		entry := cfgEntry.(map[interface{}]interface{})

		var name string
		var backend string
		var config interface{}

		if name, ok = entry["name"].(string); !ok {
			logger.Fatalf("\tplugins.config.auth[%d] must contain key \"name\"", idx)
		}

		if backend, ok = entry["backend"].(string); !ok {
			logger.Fatalf("\tplugins.config.auth[%d] must contain key \"backend\"", idx)
		}

		if config, ok = entry["config"]; !ok {
			logger.Fatalf("\tplugins.config.auth[%d] must contain map \"config\"", idx)
		}

		var iface vlauth.Iface

		switch backend {
		case "simpleAuth":
			iface, _ = configureSimpleAuth(config)
		default:
			var authPlugins pluginType
			if authPlugins, ok = plTypes["auth"]; ok {
				if pl, kk := authPlugins[backend]; kk {
					plObject, err := configurePlugin(pl, config)
					if err != nil {
						logger.Fatalf(err.Error())
						return nil, errors.New("")
					}

					iface = plObject.(vlauth.Iface)
				} else {
					logger.Warnf("\tno enabled plugin of type [%d] for config [%s]", backend, name)
				}
			} else {
				logger.Error("\tno auth plugins loaded")
			}
		}

		if err := auth.Register(name, iface); err != nil {
			logger.Error("\tauth provider", zap.String("name", name), zap.String("backend", backend), zap.Error(err))

			return nil, err
		}
	}

	def, err := auth.NewManager(cfg.Auth.Order, cfg.Auth.Anonymous)

	if err != nil {
		logger.Error("\tcreating default auth:", err.Error())
		return nil, err
	}

	logger.Info("\tdefault auth order: ", cfg.Auth.Order)
	logger.Info("\tdefault auth anonymous: ", cfg.Auth.Anonymous)

	return def, nil
}

func loadPersistence(cfg interface{}, plTypes pluginTypes) (persistence.Provider, error) {
	persist := persistence.Default()

	logger.Info("loading persistence")
	if cfg == nil {
		logger.Warn("\tno persistence backend provided\n\tusing in-memory. Data will be lost on shutdown")
	} else {
		persistenceConfigs := cfg.([]interface{})
		if len(persistenceConfigs) > 1 {
			logger.Warn("\tplugins.config.persistence: multiple persistence providers not supported")
			return nil, errors.New("")
		}

		persistenceConfig := persistenceConfigs[0].(map[interface{}]interface{})
		var ok bool
		var backend string
		var config interface{}

		if backend, ok = persistenceConfig["backend"].(string); !ok {
			logger.Error("\tplugins.config.persistence[0] must contain key \"backend\"")
			return nil, errors.New("")
		}

		if config, ok = persistenceConfig["config"]; !ok {
			logger.Fatalf("\tplugins.config.persistence[0] must contain map \"config\"")
		}

		if plTypes != nil {
			if pl, kk := plTypes["persistence"][backend]; !kk {
				logger.Fatalf("\tplugins.config.persistence.backend: plugin type [%s] not found", backend)
				return nil, errors.New("")
			} else {
				plObject, err := configurePlugin(pl, config)
				if err != nil {
					logger.Fatalf(err.Error())
					return nil, errors.New("")
				}

				logger.Infof("\tusing persistence provider [%s]", backend)

				persist = plObject.(persistence.Provider)
			}
		}
	}

	return persist, nil
}

func configurePlugin(pl vlplugin.Plugin, c interface{}) (interface{}, error) {
	name := "plugin." + pl.Info().Type() + "." + pl.Info().Name()

	sysParams := &vlplugin.SysParams{
		SignalFailure: pluginFailureSignal,
		Log:           configuration.GetLogger().Named(name),
	}

	var err error
	var plObject interface{}

	if plObject, err = pl.Load(c, sysParams); err != nil {
		return nil, errors.New(name + ": acquire failed : " + err.Error())
	}

	return plObject, err
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
				apiV, plV := pl.Plugin.Info().Version()
				logger.Info("\t", name, ":",
					"\n\t\tPlugins API Version: ", apiV,
					"\n\t\t     Plugin Version: ", plV,
					"\n\t\t               Type: ", pl.Plugin.Info().Type(),
					"\n\t\t        Description: ", pl.Plugin.Info().Desc(),
					"\n\t\t               Name: ", pl.Plugin.Info().Name())
				if _, ok := plTypes[pl.Plugin.Info().Type()]; !ok {
					plTypes[pl.Plugin.Info().Type()] = make(map[string]vlplugin.Plugin)
				}

				plTypes[pl.Plugin.Info().Type()][pl.Plugin.Info().Name()] = pl.Plugin
			}
		}
	}

	return plTypes, nil
}

var logger *zap.SugaredLogger

func main() {
	defer func() {
		logger.Info("service stopped")

		if r := recover(); r != nil {
			logger.Panic(r)
		}
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
		OnDuplicate: func(s string, b bool) {
			logger.Info("Session duplicate: ClientID: ", s, " allowed: ", b)
		},
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

	var profServer *http.Server

	if err == nil {
		if config.System.Profiler.Port != "" {
			profServer = &http.Server{
				Addr: ":" + config.System.Profiler.Port,
			}

			logger.Info("profiler: serving at: ", "http://"+profServer.Addr+"/debug/pprof")

			go func() {
				profServer.ListenAndServe()
			}()
		}

		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		sig := <-ch
		logger.Info("service received signal: ", sig.String())
	}

	if err = srv.Close(); err != nil {
		logger.Error("shutdown server", zap.Error(err))
	}

	if profServer != nil {
		profServer.Shutdown(context.Background())
	}
}

func pluginFailureSignal(name, msg string) {
	logger.Error("plugin: ", name, ": ", msg)
}
