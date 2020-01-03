package main

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/VolantMQ/vlapi/vlauth"
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

func (a *simpleAuth) Password(clientID, user, password string) error {
	if hash, ok := a.creds[user]; ok {
		algo := sha256.New()
		if _, err := algo.Write([]byte(password)); err == nil {
			if hex.EncodeToString(algo.Sum(nil)) == hash {
				return vlauth.StatusAllow
			}
		}
	}
	return vlauth.StatusDeny
}

func (a *simpleAuth) ACL(clientID, user, topic string, access vlauth.AccessType) error {
	return vlauth.StatusAllow
}

func (a *simpleAuth) Shutdown() error {
	a.creds = nil
	return nil
}
