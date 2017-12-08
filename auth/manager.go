package auth

import (
	"errors"
	"fmt"
	"strings"
)

// Manager auth
type Manager struct {
	p []Provider
}

var providers = make(map[string]Provider)

// Register auth provider
func Register(name string, provider Provider) error {
	if name == "" && provider == nil {
		return errors.New("invalid args")
	}

	if _, dup := providers[name]; dup {
		return errors.New("already exists")
	}

	providers[name] = provider

	return nil
}

// UnRegister authenticator
func UnRegister(name string) {
	delete(providers, name)
}

// NewManager new auth manager
func NewManager(p string) (*Manager, error) {
	m := Manager{}

	list := strings.Split(p, ";")
	for _, pa := range list {
		pvd, ok := providers[pa]
		if !ok {
			return nil, fmt.Errorf("session: unknown provider %q", pa)
		}

		m.p = append(m.p, pvd)
	}

	return &m, nil
}

// Password authentication
func (m *Manager) Password(user, password string) Status {
	for _, p := range m.p {
		if status := p.Password(user, password); status == StatusAllow {
			return status
		}
	}

	return StatusDeny
}

// ACL check permissions
func (m *Manager) ACL(clientID, user, topic string, access AccessType) Status {
	for _, p := range m.p {
		if status := p.ACL(clientID, user, topic, access); status == StatusAllow {
			return status
		}
	}

	return StatusDeny
}
