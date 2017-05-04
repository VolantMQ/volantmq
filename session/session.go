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
	"sync"

	"errors"
	"github.com/juju/loggo"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/topics"
)

const (
	// Queue size for the ack queue
	defaultQueueSize = 16
)

var (
	// ErrNotInitialized session not initialized
	ErrNotInitialized = errors.New("session: not initialized yet")

	// ErrAlreadyInitialized session already initialized
	ErrAlreadyInitialized = errors.New("session: already initialized")
)

// Type session
type Type struct {
	// Pub1ack queue for outgoing PUBLISH QoS 1 messages
	Pub1ack *AckQueue

	// Pub2in queue for incoming PUBLISH QoS 2 messages
	Pub2in *AckQueue

	// Pub2out queue for outgoing PUBLISH QoS 2 messages
	Pub2out *AckQueue

	// SubAck queue for outgoing SUBSCRIBE messages
	SubAck *AckQueue

	// UnSubAck queue for outgoing UNSUBSCRIBE messages
	UnSubAck *AckQueue

	// PingAck queue for outgoing PINGREQ messages
	PingAck *AckQueue

	// Will message to publish if connect is closed unexpectedly
	Will *message.PublishMessage

	// Retained publish message
	Retained *message.PublishMessage

	// topics stores all the topics for this session/client
	topics topics.Topics

	// Serialize access to this session
	mu sync.Mutex

	id string

	clean bool

	// Initialized?
	initialized bool
}

var appLog loggo.Logger

func init() {
	appLog = loggo.GetLogger("mq.session")
	appLog.SetLogLevel(loggo.INFO)
}

// Init session
func (s *Type) Init(msg *message.ConnectMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.initialized {
		return ErrAlreadyInitialized
	}

	s.update(msg)

	s.topics = make(map[string]byte, 1)

	s.id = string(msg.ClientID())

	s.Pub1ack = newAckQueue(defaultQueueSize)
	s.Pub2in = newAckQueue(defaultQueueSize)
	s.Pub2out = newAckQueue(defaultQueueSize)
	s.SubAck = newAckQueue(defaultQueueSize)
	s.UnSubAck = newAckQueue(defaultQueueSize)
	s.PingAck = newAckQueue(defaultQueueSize)

	s.initialized = true

	return nil
}

func (s *Type) update(msg *message.ConnectMessage) {
	if msg.WillFlag() {
		s.Will = message.NewPublishMessage()
		s.Will.SetQoS(msg.WillQos())             // nolint: errcheck
		s.Will.SetTopic(string(msg.WillTopic())) // nolint: errcheck
		s.Will.SetPayload(msg.WillMessage())
		s.Will.SetRetain(msg.WillRetain())
	}

	s.clean = msg.CleanSession()
}

// Update message
func (s *Type) Update(msg *message.ConnectMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.update(msg)

	return nil
}

// RetainMessage message
func (s *Type) RetainMessage(msg *message.PublishMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return ErrNotInitialized
	}

	rMsg := *msg

	s.Retained = &rMsg

	return nil
}

// AddTopic add topic
func (s *Type) AddTopic(topic string, qos byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return ErrNotInitialized
	}

	s.topics[topic] = qos

	return nil
}

// RemoveTopic remove
func (s *Type) RemoveTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return ErrNotInitialized
	}

	delete(s.topics, topic)

	return nil
}

// Topics list topics
func (s *Type) Topics() (*topics.Topics, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return nil, ErrNotInitialized
	}

	topics := s.topics

	return &topics, nil
}

// ID session id
func (s *Type) ID() string {
	return s.id
}

// IsClean is session clean or not
func (s *Type) IsClean() bool {
	return s.clean
}
