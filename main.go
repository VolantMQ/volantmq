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
	"strings"
	"sync"
	"syscall"

	"github.com/VolantMQ/vlapi/plugin"
	"github.com/VolantMQ/vlapi/plugin/auth"
	"github.com/VolantMQ/vlapi/plugin/persistence"
	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/server"
	"github.com/VolantMQ/volantmq/transport"
	"github.com/troian/healthcheck"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type pluginType map[string]vlplugin.Plugin

type pluginTypes map[string]pluginType

type httpServer struct {
	mux    *http.ServeMux
	server *http.Server
}

// Mux ...
func (h *httpServer) Mux() *http.ServeMux {
	return h.mux
}

// Addr ...
func (h *httpServer) Addr() string {
	return h.server.Addr
}

type appContext struct {
	plugins struct {
		acquired   pluginTypes
		configured []interface{}
	}

	httpDefaultMux string
	httpServers    sync.Map

	healthLock      sync.Mutex
	healthHandler   healthcheck.Handler
	livenessChecks  map[string]healthcheck.Check
	readinessChecks map[string]healthcheck.Check
}

var _ healthcheck.Checks = (*appContext)(nil)

var logger *zap.SugaredLogger

// those are provided at compile time

// GitCommit SHA hash
var GitCommit string

// GitBranch if any
var GitBranch string

// GitState repository state
var GitState string

// GitSummary repository info
var GitSummary string

// BuildDate build date
var BuildDate string

// Version application version
var Version string

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

func (ctx *appContext) loadPlugins(cfg *configuration.PluginsConfig) error {
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

	ctx.plugins.acquired = plTypes
	return nil
}

func configureSimpleAuth(cfg interface{}) (vlauth.IFace, error) {
	sAuth := newSimpleAuth()
	authConfig := cfg.(map[interface{}]interface{})

	if list, kk := authConfig["users"].(map[interface{}]interface{}); kk {
		for u, p := range list {
			sAuth.addUser(u.(string), strings.ToLower(p.(string)))
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

func (ctx *appContext) loadAuth(cfg *configuration.Config) (*auth.Manager, error) {
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

		var iface vlauth.IFace

		switch backend {
		case "simpleAuth":
			iface, _ = configureSimpleAuth(config)
		default:
			var authPlugins pluginType
			if authPlugins, ok = ctx.plugins.acquired["auth"]; ok {
				if pl, kk := authPlugins[backend]; kk {
					plObject, err := ctx.configurePlugin(pl, config)
					if err != nil {
						logger.Fatalf(err.Error())
						return nil, errors.New("")
					}

					iface = plObject.(vlauth.IFace)
				} else {
					logger.Warnf("\tno enabled plugin of type [%s] for config [%s]", backend, name)
				}
			} else {
				logger.Warn("\tno auth plugins loaded")
			}
		}

		if iface != nil {
			if err := auth.Register(name, iface); err != nil {
				logger.Error("\tauth provider", zap.String("name", name), zap.String("backend", backend), zap.Error(err))
				return nil, err
			}
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

func (ctx *appContext) configureDebugPlugins(cfg interface{}) error {
	logger.Info("configuring debug plugins")

	for idx, cfgEntry := range cfg.([]interface{}) {
		entry := cfgEntry.(map[interface{}]interface{})

		var backend string
		var config interface{}
		var ok bool

		if backend, ok = entry["backend"].(string); !ok {
			logger.Fatalf("\tplugins.config.debug[%d] must contain key \"backend\"", idx)
		}

		if config, ok = entry["config"]; !ok {
			logger.Fatalf("\tplugins.config.debug[%d] must contain map \"config\"", idx)
		}

		var debugPlugins pluginType
		if debugPlugins, ok = ctx.plugins.acquired["debug"]; ok {
			if pl, kk := debugPlugins[backend]; kk {
				_, err := ctx.configurePlugin(pl, config)
				if err != nil {
					logger.Fatalf(err.Error())
					return errors.New("")
				}
			} else {
				logger.Warnf("\tno enabled plugin of type [%d]", backend)
			}
		} else {
			logger.Warn("\tno debug plugins loaded")
		}
	}
	return nil
}

func (ctx *appContext) configureHealthPlugins(cfg interface{}) error {
	logger.Info("configuring health plugins")

	for idx, cfgEntry := range cfg.([]interface{}) {
		entry := cfgEntry.(map[interface{}]interface{})

		var backend string
		var config interface{}
		var ok bool

		if backend, ok = entry["backend"].(string); !ok {
			logger.Fatalf("\tplugins.config.health[%d] must contain key \"backend\"", idx)
		}

		if config, ok = entry["config"]; !ok {
			logger.Fatalf("\tplugins.config.health[%d] must contain map \"config\"", idx)
		}

		var debugPlugins pluginType
		if debugPlugins, ok = ctx.plugins.acquired["health"]; ok {
			if pl, kk := debugPlugins[backend]; kk {
				plObject, err := ctx.configurePlugin(pl, config)
				if err != nil {
					logger.Fatalf(err.Error())
					return errors.New("")
				}

				ctx.healthLock.Lock()
				ctx.healthHandler = plObject.(healthcheck.Handler)

				// if any checks already registered move them to the plugin
				for n, c := range ctx.livenessChecks {
					ctx.healthHandler.AddLivenessCheck(n, c)
				}

				for n, c := range ctx.readinessChecks {
					ctx.healthHandler.AddReadinessCheck(n, c)
				}

				ctx.livenessChecks = nil
				ctx.readinessChecks = nil

				ctx.healthLock.Unlock()
			} else {
				logger.Warnf("\tno enabled plugin of type [%d]", backend)
			}
		} else {
			logger.Warn("\tno health plugins loaded")
		}
	}
	return nil
}

func (ctx *appContext) loadPersistence(cfg interface{}) (persistence.IFace, error) {
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

		if ctx.plugins.acquired != nil {
			if pl, kk := ctx.plugins.acquired["persistence"][backend]; !kk {
				logger.Fatalf("\tplugins.config.persistence.backend: plugin type [%s] not found", backend)
				return nil, errors.New("")
			} else {
				plObject, err := ctx.configurePlugin(pl, config)
				if err != nil {
					logger.Fatalf(err.Error())
					return nil, errors.New("")
				}

				logger.Infof("\tusing persistence provider [%s]", backend)

				persist = plObject.(persistence.IFace)
			}
		}
	}

	return persist, nil
}

func (ctx *appContext) configurePlugin(pl vlplugin.Plugin, c interface{}) (interface{}, error) {
	name := "plugin." + pl.Info().Type() + "." + pl.Info().Name()

	sysParams := &vlplugin.SysParams{
		HTTP:          ctx,
		Health:        ctx,
		SignalFailure: pluginFailureSignal,
		Log:           configuration.GetLogger().Named(name),
	}

	var err error
	var plObject interface{}

	if plObject, err = pl.Load(c, sysParams); err != nil {
		return nil, errors.New(name + ": acquire failed : " + err.Error())
	}

	ctx.plugins.configured = append(ctx.plugins.configured, plObject)

	return plObject, err
}

// GetHTTPServer ...
func (ctx *appContext) GetHTTPServer(port string) vlplugin.HTTPHandler {
	if port == "" {
		port = ctx.httpDefaultMux
	}

	srv := &httpServer{
		mux: http.NewServeMux(),
	}

	srv.server = &http.Server{
		Addr:    ":" + port,
		Handler: srv.mux,
	}

	if actual, ok := ctx.httpServers.LoadOrStore(port, srv); ok {
		srv = actual.(*httpServer)
	}

	return srv
}

// GetHealth ...
func (ctx *appContext) GetHealth() healthcheck.Checks {
	return ctx
}

// AddLivenessCheck ...
func (ctx *appContext) AddLivenessCheck(name string, check healthcheck.Check) error {
	ctx.healthLock.Lock()
	defer ctx.healthLock.Unlock()

	if ctx.healthHandler == nil {
		ctx.livenessChecks[name] = check
	} else {
		ctx.healthHandler.AddLivenessCheck(name, check)
	}

	return nil
}

// AddReadinessCheck ...
func (ctx *appContext) AddReadinessCheck(name string, check healthcheck.Check) error {
	ctx.healthLock.Lock()
	defer ctx.healthLock.Unlock()

	if ctx.healthHandler == nil {
		ctx.readinessChecks[name] = check
	} else {
		ctx.healthHandler.AddReadinessCheck(name, check)
	}

	return nil
}

// RemoveLivenessCheck ...
func (ctx *appContext) RemoveLivenessCheck(name string) error {
	ctx.healthLock.Lock()
	defer ctx.healthLock.Unlock()

	if ctx.healthHandler == nil {
		delete(ctx.livenessChecks, name)
	} else {
		ctx.healthHandler.RemoveLivenessCheck(name)
	}

	return nil
}

// RemoveReadinessCheck ...
func (ctx *appContext) RemoveReadinessCheck(name string) error {
	ctx.healthLock.Lock()
	defer ctx.healthLock.Unlock()

	if ctx.healthHandler == nil {
		delete(ctx.readinessChecks, name)
	} else {
		ctx.healthHandler.RemoveReadinessCheck(name)
	}

	return nil
}

func main() {
	defer func() {
		logger.Info("service stopped")

		if r := recover(); r != nil {
			logger.Panic(r)
		}
	}()

	logger = configuration.GetHumanLogger()
	logger.Info("starting service...")
	logger.Infof("\n\tbuild info:\n"+
		"\t\tcommit : %s\n"+
		"\t\tbranch : %s\n"+
		"\t\tstate  : %s\n"+
		"\t\tsummary: %s\n"+
		"\t\tdate   : %s\n"+
		"\t\tversion: %s\n", GitCommit, GitBranch, GitState, GitSummary, BuildDate, Version)
	config := configuration.ReadConfig()
	if config == nil {
		return
	}

	if err := configuration.ConfigureLoggers(&config.System.Log); err != nil {
		return
	}

	logger = configuration.GetHumanLogger()

	logger.Info("working directory: ", configuration.WorkDir)
	logger.Info("plugins directory: ", configuration.PluginsDir)

	var persist persistence.IFace
	var defaultAuth *auth.Manager
	var err error

	ctx := &appContext{
		httpDefaultMux:  config.System.Http.DefaultPort,
		livenessChecks:  make(map[string]healthcheck.Check),
		readinessChecks: make(map[string]healthcheck.Check),
	}

	if err = ctx.loadPlugins(&config.Plugins); err != nil {
		return
	}

	if c, ok := config.Plugins.Config["debug"]; ok {
		if err = ctx.configureDebugPlugins(c); err != nil {
			logger.Error("loading debug plugins", zap.Error(err))
			return
		}
	}
	if c, ok := config.Plugins.Config["health"]; ok {
		if err = ctx.configureHealthPlugins(c); err != nil {
			logger.Error("loading health plugins", zap.Error(err))
			return
		}
	}

	if persist, err = ctx.loadPersistence(config.Plugins.Config["persistence"]); err != nil {
		logger.Error("loading persistence plugins", zap.Error(err))
		return
	}

	if defaultAuth, err = ctx.loadAuth(config); err != nil {
		logger.Error("loading auth plugins", zap.Error(err))
		return
	}

	ctx.httpServers.Range(func(k, v interface{}) bool {
		s := v.(*httpServer)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Panic("http server panic", zap.Any("panic", r))
				}
			}()
			logger.Info("starting http server on " + s.server.Addr)
			s.server.ListenAndServe()
			logger.Info("stopped http server on " + s.server.Addr)
		}()
		return true
	})

	var listeners map[string][]interface{}

	if listeners, err = loadListeners(defaultAuth, &config.Listeners); err != nil {
		logger.Error("loading listeners", zap.Error(err))
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
		Health:          ctx,
		MQTT:            config.Mqtt,
		Persistence:     persist,
		TransportStatus: listenerStatus,
		OnDuplicate: func(s string, b bool) {
			logger.Info("Session duplicate: clientId: ", s, " allowed: ", b)
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

	// stop if any listeners failed
	if err != nil {
		return
	}

	if err == nil {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		sig := <-ch
		logger.Info("service received signal: ", sig.String())
	}

	if err = srv.Shutdown(); err != nil {
		logger.Error("shutdown server", zap.Error(err))
	}

	ctx.httpServers.Range(func(k, v interface{}) bool {
		s := v.(*httpServer)
		s.server.Shutdown(context.Background())
		return true
	})
}

func pluginFailureSignal(name, msg string) {
	logger.Error("plugin: ", name, ": ", msg)
}
