package auth

import (
	"errors"
	"fmt"

	"github.com/VolantMQ/vlapi/plugin/auth"
)

// Manager auth
type Manager struct {
	p         []vlauth.IFace
	anonymous bool
}

var providers = make(map[string]vlauth.IFace)

// Register auth provider
func Register(name string, i vlauth.IFace) error {
	if name == "" && i == nil {
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
func NewManager(p []string, allowAnonymous bool) (*Manager, error) {
	m := Manager{
		anonymous: allowAnonymous,
	}

	for _, pa := range p {
		pvd, ok := providers[pa]
		if !ok {
			return nil, fmt.Errorf("session: unknown provider %q", pa)
		}

		m.p = append(m.p, pvd)
	}

	return &m, nil
}

func (m *Manager) AllowAnonymous() error {
	if m.anonymous {
		return vlauth.StatusAllow
	}

	return vlauth.StatusDeny
}

// Password authentication
func (m *Manager) Password(clientID, user, password string) error {
	if user == "" && m.anonymous {
		return vlauth.StatusAllow
	} else {
		for _, p := range m.p {
			if status := p.Password(clientID, user, password); status == vlauth.StatusAllow {
				return status
			}
		}
	}

	return vlauth.StatusDeny
}

// ACL check permissions
func (m *Manager) ACL(clientID, user, topic string, access vlauth.AccessType) error {
	for _, p := range m.p {
		if status := p.ACL(clientID, user, topic, access); status == vlauth.StatusAllow {
			return status
		}
	}

	return vlauth.StatusDeny
}
