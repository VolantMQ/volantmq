package auth

import (
	"errors"
	"fmt"

	"github.com/VolantMQ/vlapi/vlauth"
)

// Manager auth
type Manager struct {
	p []vlauth.IFace
}

var providers = make(map[string]vlauth.IFace)

// Register auth provider
func Register(name string, i vlauth.IFace) error {
	if name == "" || i == nil {
		return errors.New("invalid args")
	}

	if _, dup := providers[name]; dup {
		return errors.New("already exists")
	}

	providers[name] = i

	return nil
}

// UnRegister authenticator
func UnRegister(name string) {
	delete(providers, name)
}

// NewManager new auth manager
func NewManager(p []string) (*Manager, error) {
	m := &Manager{}

	for _, pa := range p {
		pvd, ok := providers[pa]
		if !ok {
			return nil, fmt.Errorf("session: unknown provider %q", pa)
		}

		m.p = append(m.p, pvd)
	}

	return m, nil
}

// Password authentication
func (m *Manager) Password(clientID, user, password string) (vlauth.Permissions, error) {
	for _, p := range m.p {
		if status := p.Password(clientID, user, password); status == vlauth.StatusAllow {
			return p, status
		}
	}

	return nil, vlauth.StatusDeny
}
