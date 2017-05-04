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

package session

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"sync"
)

// Manager interface
type Manager struct {
	sessions map[string]*Type
	mu       sync.RWMutex
}

var (
	// ErrNoSuchSession given session does not exist
	ErrNoSuchSession = errors.New("given session does not exist")
)

// NewManager alloc new
func NewManager() (*Manager, error) {
	return &Manager{sessions: make(map[string]*Type)}, nil
}

// New session
func (m *Manager) New(id string) (*Type, error) {
	if id == "" {
		id = m.genSessionID()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.sessions[id] = &Type{id: id}
	return m.sessions[id], nil
}

// Get session
func (m *Manager) Get(id string) (*Type, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if s, ok := m.sessions[id]; ok {
		return s, nil
	}

	return nil, ErrNoSuchSession
}

// Del session
func (m *Manager) Del(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, id)
}

// Save session
func (m *Manager) Save(id string) error {
	return nil
}

// Count sessions
func (m *Manager) Count() int {
	return len(m.sessions)
}

// Shutdown manager
func (m *Manager) Shutdown() error {
	m.sessions = make(map[string]*Type)

	return nil
}

func (m *Manager) genSessionID() string {
	b := make([]byte, 15)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}

	return base64.URLEncoding.EncodeToString(b)
}
