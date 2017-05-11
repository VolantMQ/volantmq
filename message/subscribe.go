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
	"errors"
	"fmt"
	"github.com/troian/surgemq/buffer"
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

var _ Provider = (*SubscribeMessage)(nil)

// NewSubscribeMessage creates a new SUBSCRIBE message.
func NewSubscribeMessage() *SubscribeMessage {
	msg := &SubscribeMessage{
		topics: make(TopicsQoS),
	}
	msg.SetType(SUBSCRIBE) // nolint: errcheck

	return msg
}

func (msg *SubscribeMessage) String() string {
	msgStr := fmt.Sprintf("%s, Packet ID=%d", msg.header, msg.PacketID())

	for t, q := range msg.topics {
		msgStr = fmt.Sprintf("%s, Topic=%q/%d", msgStr, t, q)
	}

	return msgStr
}

// Topics returns a list of topics sent by the Client.
func (msg *SubscribeMessage) Topics() Topics {
	topics := Topics{}

	for t := range msg.topics {
		topics = append(topics, t)
	}

	return topics
}

// AddTopic adds a single topic to the message, along with the corresponding QoS.
// An error is returned if QoS is invalid.
func (msg *SubscribeMessage) AddTopic(topic string, qos QosType) error {
	if !qos.IsValid() {
		return ErrInvalidQoS
	}

	// if topic exists, update QoS else new entry will be created thus message is dirty
	//if _, ok := msg.topics[topic]; !ok {
	//
	//}

	msg.topics[topic] = qos
	msg.dirty = true

	return nil
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (msg *SubscribeMessage) RemoveTopic(topic string) {
	if _, ok := msg.topics[topic]; ok {
		delete(msg.topics, topic)
		msg.dirty = true
	}
}

// TopicExists checks to see if a topic exists in the list.
func (msg *SubscribeMessage) TopicExists(topic string) bool {
	if _, ok := msg.topics[topic]; ok {
		return true
	}

	return false
}

// TopicQos returns the QoS level of a topic. If topic does not exist, QosFailure
// is returned.
func (msg *SubscribeMessage) TopicQos(topic string) QosType {

	if _, ok := msg.topics[topic]; ok {
		return msg.topics[topic]
	}

	return QosFailure
}

// Qos returns the list of QoS current in the message.
func (msg *SubscribeMessage) Qos() []QosType {
	qos := []QosType{}

	for _, q := range msg.topics {
		qos = append(qos, q)
	}

	return qos
}

// Len of message
func (msg *SubscribeMessage) Len() int {
	if !msg.dirty {
		return len(msg.dBuf)
	}

	ml := msg.msgLen()

	if err := msg.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return msg.header.msgLen() + ml
}

// Decode message
func (msg *SubscribeMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := msg.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	//this.packetId = binary.BigEndian.Uint16(src[total:])
	msg.packetID = src[total : total+2]
	total += 2

	remlen := int(msg.remLen) - (total - hn)
	for remlen > 0 {
		t, n, err := readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		msg.topics[string(t)] = QosType(src[total])
		total++

		remlen = remlen - n - 1
	}

	if len(msg.topics) == 0 {
		return 0, errors.New("subscribe/Decode: Empty topic list")
	}

	msg.dirty = false

	return total, nil
}

// Encode message
func (msg *SubscribeMessage) Encode(dst []byte) (int, error) {
	expectedSize := msg.Len()
	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	var err error
	total := 0

	if !msg.dirty {
		total = copy(dst, msg.dBuf)
	} else {
		var n int

		if n, err = msg.header.encode(dst[total:]); err != nil {
			return total, err
		}
		total += n

		if msg.PacketID() == 0 {
			msg.SetPacketID(uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff))
		}

		total += copy(dst[total:], msg.packetID)

		for t, q := range msg.topics {
			n, err = writeLPBytes(dst[total:], []byte(t))
			total += n
			if err != nil {
				return total, err
			}

			dst[total] = byte(q)
			total++
		}
	}

	return total, err
}

// Send encode and send message into ring buffer
func (msg *SubscribeMessage) Send(to *buffer.Type) (int, error) {
	var err error
	total := 0

	if !msg.dirty {
		total, err = to.Send(msg.dBuf)
	} else {
		expectedSize := msg.Len()
		if len(to.ExternalBuf) < expectedSize {
			to.ExternalBuf = make([]byte, expectedSize)
		}

		var n int

		if n, err = msg.header.encode(to.ExternalBuf[total:]); err != nil {
			return 0, err
		}
		total += n

		if msg.PacketID() == 0 {
			msg.SetPacketID(uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff))
		}

		if copy(to.ExternalBuf[total:total+2], msg.packetID) != 2 {
			to.ExternalBuf[total] = 0
			to.ExternalBuf[total+1] = 0
		}

		total += 2

		for t, q := range msg.topics {
			n, err = writeLPBytes(to.ExternalBuf[total:], []byte(t))
			total += n
			if err != nil {
				return 0, err
			}

			to.ExternalBuf[total] = byte(q)
			total++
		}

		total, err = to.Send(to.ExternalBuf[:total])
	}

	return total, err
}

func (msg *SubscribeMessage) msgLen() int {
	// packet ID
	total := 2

	for t := range msg.topics {
		total += 2 + len(t) + 1
	}

	return total
}
