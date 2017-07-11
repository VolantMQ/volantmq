// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
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
	//"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	_ "runtime/debug"
	"syscall"

	"github.com/spf13/viper"
	"github.com/troian/surgemq"
	"github.com/troian/surgemq/auth"
	authTypes "github.com/troian/surgemq/auth/types"
	persistType "github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/server"
	_ "github.com/troian/surgemq/topics/mem"
	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
)

type internalAuth struct {
	creds map[string]string
}

func (a internalAuth) Password(user, password string) error {
	if hash, ok := a.creds[user]; ok {
		if password == hash {
			return nil
		}
	}
	return auth.ErrAuthFailure
}

// nolint: golint
func (a internalAuth) AclCheck(clientID, user, topic string, access authTypes.AccessType) error {
	return auth.ErrAuthFailure
}

func (a internalAuth) PskKey(hint, identity string, key []byte, maxKeyLen int) error {
	return auth.ErrAuthFailure
}

func main() {
	ops := surgemq.Options{
		LogWithTs: false,
	}

	surgemq.Init(ops)

	logger := surgemq.GetProdLogger().Named("example")

	defer func() {
		if r := recover(); r != nil {
			logger.Error("Recover from panic", zap.Any("recover", r))
		}
	}()

	var err error

	logger.Info("Starting application")

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

	var srv server.Type

	listenerStatus := func(id string, start bool) {
		if start {
			logger.Info("Started listener", zap.String("id", id))
		} else {
			logger.Info("Stopped listener", zap.String("id", id))
		}
	}

	srv, err = server.New(server.Config{
		KeepAlive:      types.DefaultKeepAlive,
		AckTimeout:     types.DefaultAckTimeout,
		ConnectTimeout: 5,
		TimeoutRetries: types.DefaultTimeoutRetries,
		TopicsProvider: types.DefaultTopicsProvider,
		Authenticators: "internal",
		Anonymous:      true,
		Persistence: &persistType.BoltDBConfig{
			File: "./persist.db",
		},
		DupConfig: types.DuplicateConfig{
			Replace:   true,
			OnAttempt: nil,
		},
		ListenerStatus: listenerStatus,
	})
	if err != nil {
		logger.Error("Couldn't create server", zap.Error(err))
		os.Exit(1)
	}

	var authMng *auth.Manager

	if authMng, err = auth.NewManager("internal"); err != nil {
		logger.Error("Couldn't register *amqp* auth provider", zap.Error(err))
		return
	}

	config := &server.ListenerTCP{
		Scheme: "tcp4",
		Host:   "",
		ListenerBase: server.ListenerBase{
			Port:        1883,
			AuthManager: authMng,
		},
	}

	if err = srv.ListenAndServe(config); err != nil {
		logger.Error("Couldn't start listener", zap.Error(err))
	}

	configWs := &server.ListenerWS{
		ListenerBase: server.ListenerBase{
			Port:        8080,
			AuthManager: authMng,
		},
	}

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
