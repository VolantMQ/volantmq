package auth

import (
	"errors"
	"fmt"

	"github.com/VolantMQ/auth"
)

// Manager auth
type Manager struct {
	p []auth.Provider
}

var providers = make(map[string]auth.Provider)

// Register auth provider
func Register(name string, provider auth.Provider) error {
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
func NewManager(p []string) (*Manager, error) {
	m := Manager{}

	for _, pa := range p {
		pvd, ok := providers[pa]
		if !ok {
			return nil, fmt.Errorf("session: unknown provider %q", pa)
		}

		m.p = append(m.p, pvd)
	}

	return &m, nil
}

// Password authentication
func (m *Manager) Password(user, password string) auth.Status {
	for _, p := range m.p {
		if status := p.Password(user, password); status == auth.StatusAllow {
			return status
		}
	}

	return auth.StatusDeny
}

// ACL check permissions
func (m *Manager) ACL(clientID, user, topic string, access auth.AccessType) auth.Status {
	for _, p := range m.p {
		if status := p.ACL(clientID, user, topic, access); status == auth.StatusAllow {
			return status
		}
	}

	return auth.StatusDeny
}
