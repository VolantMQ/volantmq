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
	"fmt"
	"sync"
)

type memProvider struct {
	st map[string]*Session
	mu sync.RWMutex
}

func init() {
	Register("mem", NewMemProvider())
}

// NewMemProvider new provider
func NewMemProvider() Provider {
	return &memProvider{
		st: make(map[string]*Session),
	}
}

func (m *memProvider) New(id string) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.st[id] = &Session{id: id}
	return m.st[id], nil
}

func (m *memProvider) Get(id string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sess, ok := m.st[id]
	if !ok {
		return nil, fmt.Errorf("store/Get: No session found for key %s", id)
	}

	return sess, nil
}

func (m *memProvider) Del(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.st, id)
}

func (m *memProvider) Save(id string) error {
	return nil
}

func (m *memProvider) Count() int {
	return len(m.st)
}

func (m *memProvider) Close() error {
	m.st = make(map[string]*Session)
	return nil
}
