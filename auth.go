package main

import vlAuth "github.com/VolantMQ/auth"

type internalAuth struct {
	creds map[string]string
}

// nolint: golint
func (a internalAuth) Password(user, password string) vlAuth.Status {
	if hash, ok := a.creds[user]; ok {
		if password == hash {
			return vlAuth.StatusAllow
		}
	}
	return vlAuth.StatusDeny
}

// nolint: golint
func (a internalAuth) ACL(clientID, user, topic string, access vlAuth.AccessType) vlAuth.Status {
	return vlAuth.StatusAllow
}
