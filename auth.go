package main

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/VolantMQ/vlapi/plugin/auth"
)

type simpleAuth struct {
	creds map[string]string
}

var _ vlauth.IFace = (*simpleAuth)(nil)

func newSimpleAuth() *simpleAuth {
	return &simpleAuth{
		creds: make(map[string]string),
	}
}

func (a *simpleAuth) addUser(u, p string) {
	a.creds[u] = p
}

// nolint: golint
func (a *simpleAuth) Password(clientID, user, password string) error {
	if hash, ok := a.creds[user]; ok {
		algo := sha256.New()
		algo.Write([]byte(password))
		if hex.EncodeToString(algo.Sum(nil)) == hash {
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
