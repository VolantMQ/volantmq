package types

import (
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// LogInterface inherited by internal packages to provide hierarchical logs
type LogInterface struct {
	Prod *zap.Logger
	Dev  *zap.Logger
}

// Default configs
const (
	DefaultKeepAlive        = 60 // DefaultKeepAlive default keep
	DefaultConnectTimeout   = 5  // DefaultConnectTimeout connect timeout
	DefaultMaxPacketSize    = 268435455
	DefaultReceiveMax       = 65535
	DefaultAckTimeout       = 20 // DefaultAckTimeout ack timeout
	DefaultTimeoutRetries   = 3  // DefaultTimeoutRetries retries
	MinKeepAlive            = 30
	DefaultSessionsProvider = "mem"         // DefaultSessionsProvider default session provider
	DefaultAuthenticator    = "mockSuccess" // DefaultAuthenticator default auth provider
	DefaultTopicsProvider   = "mem"         // DefaultTopicsProvider default topics provider
)

// OnceWait is an object that will perform exactly one action.
type OnceWait struct {
	val  uintptr
	wait sync.WaitGroup
	lock sync.Mutex
}

// Once is an object that will perform exactly one action.
type Once struct {
	val uintptr
}

// Do calls the function f if and only if Do is being called for the
// first time for this instance of Once. In other words, given
// 	var once Once
// if once.Do(f) is called multiple times, only the first call will invoke f,
// even if f has a different value in each invocation. A new instance of
// Once is required for each function to execute.
//
// Do is intended for initialization that must be run exactly once. Since f
// is niladic, it may be necessary to use a function literal to capture the
// arguments to a function to be invoked by Do:
// 	config.once.Do(func() { config.init(filename) })
//
// Because no call to Do returns until the one call to f returns, if f causes
// Do to be called, it will deadlock.
//
// If f panics, Do considers it to have returned; future calls of Do return
// without calling f.
func (o *OnceWait) Do(f func()) bool {
	o.lock.Lock()
	res := atomic.CompareAndSwapUintptr(&o.val, 0, 1)
	if res {
		o.wait.Add(1)
	}
	o.lock.Unlock()

	if res {
		f()
		o.wait.Done()
	} else {
		o.wait.Wait()
	}

	return res
}

// Do calls the function f if and only if Do is being called for the
// first time for this instance of Once. In other words, given
// 	var once Once
// if once.Do(f) is called multiple times, only the first call will invoke f,
// even if f has a different value in each invocation. A new instance of
// Once is required for each function to execute.
//
// Do is intended for initialization that must be run exactly once. Since f
// is niladic, it may be necessary to use a function literal to capture the
// arguments to a function to be invoked by Do:
// 	config.once.Do(func() { config.init(filename) })
//
// Because no call to Do returns until the one call to f returns, if f causes
// Do to be called, it will deadlock.
//
// If f panics, Do considers it to have returned; future calls of Do return
// without calling f.
func (o *Once) Do(f func()) bool {
	if atomic.CompareAndSwapUintptr(&o.val, 0, 1) {
		f()
		return true
	}

	return false
}
