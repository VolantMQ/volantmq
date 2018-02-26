package main

import (
	"crypto/sha256"

	"github.com/VolantMQ/vlauth"
)

type simpleAuth struct {
	creds map[string]string
}

var _ vlauth.Iface = (*simpleAuth)(nil)

func newSimpleAuth() *simpleAuth {
	return &simpleAuth{
		creds: make(map[string]string),
	}
}

func (a *simpleAuth) addUser(u, p string) {
	a.creds[u] = p
}

// nolint: golint
func (a *simpleAuth) Password(user, password string) error {
	if hash, ok := a.creds[user]; ok {
		if string(sha256.New().Sum([]byte(password))) == hash {
			return vlauth.StatusAllow
		}
	}
	return vlauth.StatusDeny
}

// nolint: golint
func (a *simpleAuth) ACL(clientID, user, topic string, access vlauth.AccessType) error {
	return vlauth.StatusAllow
}

// nolint: golint
func (a *simpleAuth) Shutdown() error {
	a.creds = nil
	return nil
}
