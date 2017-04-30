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
	"sync"

	"errors"
	"github.com/troian/surgemq/message"
)

const (
	// Queue size for the ack queue
	defaultQueueSize = 16
)

// Session session
type Session struct {
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

	// CMsg is the CONNECT message
	CMsg *message.ConnectMessage

	// Will message to publish if connect is closed unexpectedly
	Will *message.PublishMessage

	// Retained publish message
	Retained *message.PublishMessage

	// cBuf is the CONNECT message buffer, this is for storing all the will stuff
	cBuf []byte

	// rBuf is the retained PUBLISH message buffer
	rBuf []byte

	// topics stores all the topis for this session/client
	topics map[string]byte

	// Initialized?
	initialized bool

	// Serialize access to this session
	mu sync.Mutex

	id string
}

// Init message
func (s *Session) Init(msg *message.ConnectMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.initialized {
		return errors.New("Session already initialized")
	}

	s.cBuf = make([]byte, msg.Len())
	s.CMsg = message.NewConnectMessage()

	if _, err := msg.Encode(s.cBuf); err != nil {
		return err
	}

	if _, err := s.CMsg.Decode(s.cBuf); err != nil {
		return err
	}

	if s.CMsg.WillFlag() {
		s.Will = message.NewPublishMessage()
		s.Will.SetQoS(s.CMsg.WillQos())     // nolint: errcheck
		s.Will.SetTopic(s.CMsg.WillTopic()) // nolint: errcheck
		s.Will.SetPayload(s.CMsg.WillMessage())
		s.Will.SetRetain(s.CMsg.WillRetain())
	}

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

// Update message
func (s *Session) Update(msg *message.ConnectMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cBuf = make([]byte, msg.Len())
	s.CMsg = message.NewConnectMessage()

	if _, err := msg.Encode(s.cBuf); err != nil {
		return err
	}

	if _, err := s.CMsg.Decode(s.cBuf); err != nil {
		return err
	}

	return nil
}

// RetainMessage message
func (s *Session) RetainMessage(msg *message.PublishMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rBuf = make([]byte, msg.Len())
	s.Retained = message.NewPublishMessage()

	if _, err := msg.Encode(s.rBuf); err != nil {
		return err
	}

	if _, err := s.Retained.Decode(s.rBuf); err != nil {
		return err
	}

	return nil
}

// AddTopic add topic
func (s *Session) AddTopic(topic string, qos byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return errors.New("Session not yet initialized")
	}

	s.topics[topic] = qos

	return nil
}

// RemoveTopic remove
func (s *Session) RemoveTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return errors.New("Session not yet initialized")
	}

	delete(s.topics, topic)

	return nil
}

// Topics list topics
func (s *Session) Topics() ([]string, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return nil, nil, errors.New("Session not yet initialized")
	}

	var (
		topics []string
		qoss   []byte
	)

	for k, v := range s.topics {
		topics = append(topics, k)
		qoss = append(qoss, v)
	}

	return topics, qoss, nil
}

// ID session id
func (s *Session) ID() string {
	return string(s.CMsg.ClientID())
}
