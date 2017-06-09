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
	"runtime/debug"
	"syscall"

	"github.com/juju/loggo"
	"github.com/spf13/viper"
	"github.com/troian/surgemq/auth"
	authTypes "github.com/troian/surgemq/auth/types"
	persistType "github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/server"
	_ "github.com/troian/surgemq/topics/mem"
	"github.com/troian/surgemq/types"
)

var appLog loggo.Logger

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
	defer func() {
		if r := recover(); r != nil {
			appLog.Errorf("Recover from panic: %s", r)
			debug.PrintStack()
		}
	}()

	var err error

	appLog.SetLogLevel(loggo.DEBUG)
	appLog.Infof("Starting application")

	viper.SetConfigName("config")
	viper.AddConfigPath("conf")
	viper.SetConfigType("json")

	appLog.Infof("Initializing configs")
	if err = viper.ReadInConfig(); err != nil {
		appLog.Errorf("Fatal error config file: %s \n", err)
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
		appLog.Errorf("Fatal error config file: %s \n", err)
		os.Exit(1)
	}

	for i := range internalCreds {
		ia.creds[internalCreds[i].User] = internalCreds[i].Password
	}

	if err = auth.Register("internal", ia); err != nil {
		appLog.Errorf(err.Error())
		os.Exit(1)
	}

	var srv server.Type

	listenerStatus := func(id string, start bool) {
		if start {
			appLog.Infof("Started listener [%s]", id)
		} else {
			appLog.Infof("Stopped listener [%s]", id)
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
		appLog.Errorf(err.Error())
		os.Exit(1)
	}

	var authMng *auth.Manager

	if authMng, err = auth.NewManager("internal"); err != nil {
		appLog.Errorf("Couldn't register *amqp* auth provider: %s", err.Error())
		return
	}

	config := &server.Listener{
		Scheme:      "tcp4",
		Host:        "",
		Port:        1883,
		AuthManager: authMng,
	}

	if err = srv.ListenAndServe(config); err != nil {
		appLog.Errorf("%s", err.Error())
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	appLog.Warningf("Received signal: [%s]\n", <-ch)

	if err = srv.Close(); err != nil {
		appLog.Errorf(err.Error())
	}
}
