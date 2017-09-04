package main

import "github.com/VolantMQ/volantmq/auth"

type internalAuth struct {
	creds map[string]string
}

func (a internalAuth) Password(user, password string) auth.Status {
	if hash, ok := a.creds[user]; ok {
		if password == hash {
			return auth.StatusAllow
		}
	}
	return auth.StatusDeny
}

// nolint: golint
func (a internalAuth) ACL(clientID, user, topic string, access auth.AccessType) auth.Status {
	return auth.StatusAllow
}
