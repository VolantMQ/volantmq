// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sessions

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

var (
	//ErrSessionsProviderNotFound = errors.New("Session: Session provider not found")
	//ErrKeyNotAvailable          = errors.New("Session: not item found for key.")

	providers = make(map[string]Provider)
)

// Provider interface
type Provider interface {
	New(id string) (*Session, error)
	Get(id string) (*Session, error)
	Del(id string)
	Save(id string) error
	Count() int
	Close() error
}

// Register makes a session provider available by the provided name.
// If a Register is called twice with the same name or if the driver is nil,
// it panics.
func Register(name string, provider Provider) {
	if provider == nil {
		panic("session: Register provide is nil")
	}

	if _, dup := providers[name]; dup {
		panic("session: Register called twice for provider " + name)
	}

	providers[name] = provider
}

// UnRegister unregister
func UnRegister(name string) {
	delete(providers, name)
}

// Manager interface
type Manager struct {
	p Provider
}

// NewManager alloc new
func NewManager(providerName string) (*Manager, error) {
	p, ok := providers[providerName]
	if !ok {
		return nil, fmt.Errorf("session: unknown provider %q", providerName)
	}

	return &Manager{p: p}, nil
}

// New session
func (m *Manager) New(id string) (*Session, error) {
	if id == "" {
		id = m.sessionID()
	}
	return m.p.New(id)
}

// Get session
func (m *Manager) Get(id string) (*Session, error) {
	return m.p.Get(id)
}

// Del session
func (m *Manager) Del(id string) {
	m.p.Del(id)
}

// Save session
func (m *Manager) Save(id string) error {
	return m.p.Save(id)
}

// Count sessions
func (m *Manager) Count() int {
	return m.p.Count()
}

// Close manager
func (m *Manager) Close() error {
	return m.p.Close()
}

func (m *Manager) sessionID() string {
	b := make([]byte, 15)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return base64.URLEncoding.EncodeToString(b)
}
