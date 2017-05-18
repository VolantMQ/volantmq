package main

import (
	"github.com/juju/loggo"
	"github.com/troian/surgemq/persistence/bolt"
	"os"
	"os/signal"
	"syscall"
)

var appLog loggo.Logger

func main() {
	p, err := bolt.NewBolt("test.db")
	if err != nil {
		appLog.Errorf(err.Error())
		os.Exit(1)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	appLog.Warningf("Received signal: [%s]\n", <-ch)

	p.Shutdown() // nolint: errcheck, gas
}
