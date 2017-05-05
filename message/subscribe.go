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

package message

import (
	"fmt"
	"github.com/pkg/errors"
	"sync/atomic"
)

// SubscribeMessage The SUBSCRIBE Packet is sent from the Client to the Server to create one or more
// Subscriptions. Each Subscription registers a Client’s interest in one or more
// Topics. The Server sends PUBLISH Packets to the Client in order to forward
// Application Messages that were published to Topics that match these Subscriptions.
// The SUBSCRIBE Packet also specifies (for each Subscription) the maximum QoS with
// which the Server can send Application Messages to the Client.
type SubscribeMessage struct {
	header

	topics TopicsQoS
}

var _ Message = (*SubscribeMessage)(nil)

// NewSubscribeMessage creates a new SUBSCRIBE message.
func NewSubscribeMessage() *SubscribeMessage {
	msg := &SubscribeMessage{
		topics: make(TopicsQoS),
	}
	msg.SetType(SUBSCRIBE) // nolint: errcheck

	return msg
}

func (sm SubscribeMessage) String() string {
	msgStr := fmt.Sprintf("%s, Packet ID=%d", sm.header, sm.PacketID())

	for t, q := range sm.topics {
		msgStr = fmt.Sprintf("%s, Topic=%q/%d", msgStr, t, q)
	}

	return msgStr
}

// Topics returns a list of topics sent by the Client.
func (sm *SubscribeMessage) Topics() Topics {
	topics := Topics{}

	for t := range sm.topics {
		topics = append(topics, t)
	}

	return topics
}

// AddTopic adds a single topic to the message, along with the corresponding QoS.
// An error is returned if QoS is invalid.
func (sm *SubscribeMessage) AddTopic(topic string, qos QosType) error {
	if !ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}

	// if topic exists, update QoS else new entry will be created thus message is dirty
	if _, ok := sm.topics[topic]; !ok {
		sm.dirty = true
	}

	sm.topics[topic] = qos

	return nil
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (sm *SubscribeMessage) RemoveTopic(topic string) {
	if _, ok := sm.topics[topic]; ok {
		delete(sm.topics, topic)
		sm.dirty = true
	}
}

// TopicExists checks to see if a topic exists in the list.
func (sm *SubscribeMessage) TopicExists(topic string) bool {
	if _, ok := sm.topics[topic]; ok {
		return true
	}

	return false
}

// TopicQos returns the QoS level of a topic. If topic does not exist, QosFailure
// is returned.
func (sm *SubscribeMessage) TopicQos(topic string) QosType {

	if _, ok := sm.topics[topic]; ok {
		return sm.topics[topic]
	}

	return QosFailure
}

// Qos returns the list of QoS current in the message.
func (sm *SubscribeMessage) Qos() []QosType {
	qos := []QosType{}

	for _, q := range sm.topics {
		qos = append(qos, q)
	}

	return qos
}

// Len of message
func (sm *SubscribeMessage) Len() int {
	if !sm.dirty {
		return len(sm.dBuf)
	}

	ml := sm.msgLen()

	if err := sm.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return sm.header.msgLen() + ml
}

// Decode message
func (sm *SubscribeMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := sm.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	//this.packetId = binary.BigEndian.Uint16(src[total:])
	sm.packetID = src[total : total+2]
	total += 2

	remlen := int(sm.remLen) - (total - hn)
	for remlen > 0 {
		t, n, err := readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		sm.topics[string(t)] = QosType(src[total])
		total++

		remlen = remlen - n - 1
	}

	if len(sm.topics) == 0 {
		return 0, errors.New("subscribe/Decode: Empty topic list")
	}

	sm.dirty = false

	return total, nil
}

// Encode message
func (sm *SubscribeMessage) Encode(dst []byte) (int, error) {
	if !sm.dirty {
		if len(dst) < len(sm.dBuf) {
			return 0, fmt.Errorf("subscribe/Encode: Insufficient buffer size. Expecting %d, got %d", len(sm.dBuf), len(dst))
		}

		return copy(dst, sm.dBuf), nil
	}

	hl := sm.header.msgLen()
	ml := sm.msgLen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("subscribe/Encode: Insufficient buffer size. Expecting %d, got %d", hl+ml, len(dst))
	}

	if err := sm.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := sm.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	if sm.PacketID() == 0 {
		sm.SetPacketID(uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff))
		//this.packetID = uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff)
	}

	n = copy(dst[total:], sm.packetID)
	//binary.BigEndian.PutUint16(dst[total:], this.packetId)
	total += n

	for t, q := range sm.topics {
		n, err := writeLPBytes(dst[total:], []byte(t))
		total += n
		if err != nil {
			return total, err
		}

		dst[total] = byte(q)
		total++
	}

	return total, nil
}

func (sm *SubscribeMessage) msgLen() int {
	// packet ID
	total := 2

	for t := range sm.topics {
		total += 2 + len(t) + 1
	}

	return total
}
