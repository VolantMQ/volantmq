// Copyright (c) 2014 The VolantMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/VolantMQ/volantmq"
	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/transport"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	_ "net/http/pprof"
	"runtime"
	_ "runtime/debug"
)

func main() {
	ops := configuration.Options{
		LogWithTs: false,
	}

	configuration.Init(ops)

	logger := configuration.GetLogger().Named("example")

	var err error

	logger.Info("Starting application")
	logger.Info("Allocated cores", zap.Int("GOMAXPROCS", runtime.GOMAXPROCS(0)))
	viper.SetConfigName("config")
	viper.AddConfigPath("conf")
	viper.SetConfigType("json")

	logger.Info("Initializing configs")
	if err = viper.ReadInConfig(); err != nil {
		logger.Error("Couldn't read config file", zap.Error(err))
		os.Exit(1)
	}

	ia := internalAuth{
		creds: make(map[string]string),
	}

	var internalCreds []struct {
		User     string `json:"user"`
		Password string `json:"password"`
	}

	if err = viper.UnmarshalKey("mqtt.auth.internal", &internalCreds); err != nil {
		logger.Error("Couldn't unmarshal config", zap.Error(err))
		os.Exit(1)
	}

	for i := range internalCreds {
		ia.creds[internalCreds[i].User] = internalCreds[i].Password
	}

	if err = auth.Register("internal", ia); err != nil {
		logger.Error("Couldn't register *internal* auth provider", zap.Error(err))
		os.Exit(1)
	}

	var srv volantmq.Server

	listenerStatus := func(id string, status string) {
		logger.Info("Listener status", zap.String("id", id), zap.String("status", status))
	}

	serverConfig := volantmq.NewServerConfig()

	serverConfig.OfflineQoS0 = true
	serverConfig.TransportStatus = listenerStatus
	serverConfig.AllowDuplicates = true
	serverConfig.Authenticators = "internal"

	srv, err = volantmq.NewServer(serverConfig)

	if err != nil {
		logger.Error("Couldn't create server", zap.Error(err))
		os.Exit(1)
	}

	var authMng *auth.Manager

	if authMng, err = auth.NewManager("internal"); err != nil {
		logger.Error("Couldn't register *amqp* auth provider", zap.Error(err))
		return
	}

	config := transport.NewConfigTCP(
		&transport.Config{
			Port:        "1883",
			AuthManager: authMng,
		})

	if err = srv.ListenAndServe(config); err != nil {
		logger.Error("Couldn't start listener", zap.Error(err))
	}

	configWs := transport.NewConfigWS(
		&transport.Config{
			Port:        "8080",
			AuthManager: authMng,
		})

	if err = srv.ListenAndServe(configWs); err != nil {
		logger.Error("Couldn't start listener", zap.Error(err))
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	sig := <-ch
	logger.Info("Received signal", zap.String("signal", sig.String()))

	if err = srv.Close(); err != nil {
		logger.Error("Couldn't shutdown server", zap.Error(err))
	}
}
