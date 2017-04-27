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

	"github.com/troian/surgemq/message"
)

const (
	// Queue size for the ack queue
	defaultQueueSize = 16
)

// Session session
type Session struct {
	// Ack queue for outgoing PUBLISH QoS 1 messages
	Pub1ack *Ackqueue

	// Ack queue for incoming PUBLISH QoS 2 messages
	Pub2in *Ackqueue

	// Ack queue for outgoing PUBLISH QoS 2 messages
	Pub2out *Ackqueue

	// Ack queue for outgoing SUBSCRIBE messages
	Suback *Ackqueue

	// Ack queue for outgoing UNSUBSCRIBE messages
	Unsuback *Ackqueue

	// Ack queue for outgoing PINGREQ messages
	Pingack *Ackqueue

	// cmsg is the CONNECT message
	Cmsg *message.ConnectMessage

	// Will message to publish if connect is closed unexpectedly
	Will *message.PublishMessage

	// Retained publish message
	Retained *message.PublishMessage

	// cbuf is the CONNECT message buffer, this is for storing all the will stuff
	cbuf []byte

	// rbuf is the retained PUBLISH message buffer
	rbuf []byte

	// topics stores all the topis for this session/client
	topics map[string]byte

	// Initialized?
	initted bool

	// Serialize access to this session
	mu sync.Mutex

	id string
}

// Init message
func (s *Session) Init(msg *message.ConnectMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.initted {
		return fmt.Errorf("Session already initialized")
	}

	s.cbuf = make([]byte, msg.Len())
	s.Cmsg = message.NewConnectMessage()

	if _, err := msg.Encode(s.cbuf); err != nil {
		return err
	}

	if _, err := s.Cmsg.Decode(s.cbuf); err != nil {
		return err
	}

	if s.Cmsg.WillFlag() {
		s.Will = message.NewPublishMessage()
		s.Will.SetQoS(s.Cmsg.WillQos())
		s.Will.SetTopic(s.Cmsg.WillTopic())
		s.Will.SetPayload(s.Cmsg.WillMessage())
		s.Will.SetRetain(s.Cmsg.WillRetain())
	}

	s.topics = make(map[string]byte, 1)

	s.id = string(msg.ClientId())

	s.Pub1ack = newAckqueue(defaultQueueSize)
	s.Pub2in = newAckqueue(defaultQueueSize)
	s.Pub2out = newAckqueue(defaultQueueSize)
	s.Suback = newAckqueue(defaultQueueSize)
	s.Unsuback = newAckqueue(defaultQueueSize)
	s.Pingack = newAckqueue(defaultQueueSize)

	s.initted = true

	return nil
}

// Update message
func (s *Session) Update(msg *message.ConnectMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cbuf = make([]byte, msg.Len())
	s.Cmsg = message.NewConnectMessage()

	if _, err := msg.Encode(s.cbuf); err != nil {
		return err
	}

	if _, err := s.Cmsg.Decode(s.cbuf); err != nil {
		return err
	}

	return nil
}

// RetainMessage message
func (s *Session) RetainMessage(msg *message.PublishMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rbuf = make([]byte, msg.Len())
	s.Retained = message.NewPublishMessage()

	if _, err := msg.Encode(s.rbuf); err != nil {
		return err
	}

	if _, err := s.Retained.Decode(s.rbuf); err != nil {
		return err
	}

	return nil
}

// AddTopic add topic
func (s *Session) AddTopic(topic string, qos byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initted {
		return fmt.Errorf("Session not yet initialized")
	}

	s.topics[topic] = qos

	return nil
}

// RemoveTopic remove
func (s *Session) RemoveTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initted {
		return fmt.Errorf("Session not yet initialized")
	}

	delete(s.topics, topic)

	return nil
}

// Topics list topics
func (s *Session) Topics() ([]string, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initted {
		return nil, nil, fmt.Errorf("Session not yet initialized")
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
	return string(s.Cmsg.ClientId())
}
