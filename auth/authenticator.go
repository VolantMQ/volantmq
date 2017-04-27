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

package auth

import (
	"errors"
	"fmt"
)

// Auth errors
var (
	ErrAuthFailure = errors.New("auth: Authentication failure")
	//	ErrAuthProviderNotFound = errors.New("auth: Authentication provider not found")

	providers = make(map[string]Provider)
)

type AccessType int

const (
	AuthAccessTypeRead  AccessType = 1
	AuthAccessTypeWrite            = 2
)

// Authenticator interface
type Provider interface {
	Password(user, password string) error
	AclCheck(clientID, user, topic string, access AccessType) error
	PskKey(hint, identity string, key []byte, maxKeyLen int) error
}

// Register authenticator
func Register(name string, provider Provider) error {
	if name == "" && provider == nil {
		return errors.New("Invalid args")
	}

	if _, dup := providers[name]; dup {
		return errors.New("Already exists")
	}

	providers[name] = provider

	return nil
}

// UnRegister authenticator
func UnRegister(name string) {
	delete(providers, name)
}

// Manager auth
type Manager struct {
	p Provider
}

// NewManager new auth manager
func NewManager(providerName string) (*Manager, error) {
	p, ok := providers[providerName]
	if !ok {
		return nil, fmt.Errorf("session: unknown provider %q", providerName)
	}

	return &Manager{p: p}, nil
}

// Password authentication
func (m *Manager) Password(user, password string) error {
	return m.p.Password(user, password)
}

// AclCheck
func (m *Manager) AclCheck(clientID, user, topic string, access AccessType) error {
	return m.p.AclCheck(clientID, user, topic, access)
}

// PskKey
func (m *Manager) PskKey(hint, identity string, key []byte, maxKeyLen int) error {
	return m.p.PskKey(hint, identity, key, maxKeyLen)
}
