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
	"encoding/binary"
	"errors"

	"github.com/troian/surgemq/map"

	"unicode/utf8"
)

// SubscribeMessage The SUBSCRIBE Packet is sent from the Client to the Server to create one or more
// Subscriptions. Each Subscription registers a Client’s interest in one or more
// Topics. The Server sends PUBLISH Packets to the Client in order to forward
// Application Messages that were published to Topics that match these Subscriptions.
// The SUBSCRIBE Packet also specifies (for each Subscription) the maximum QoS with
// which the Server can send Application Messages to the Client.
type SubscribeMessage struct {
	header

	topics omap.Map
}

var _ Provider = (*SubscribeMessage)(nil)

// NewSubscribeMessage creates a new SUBSCRIBE message.
func NewSubscribeMessage() *SubscribeMessage {
	msg := &SubscribeMessage{
		topics: omap.New(),
	}

	msg.setType(SUBSCRIBE) // nolint: errcheck
	msg.sizeCb = msg.size

	return msg
}

// Topics returns a list of topics sent by the Client.
func (msg *SubscribeMessage) Topics() omap.Map {
	return msg.topics
}

// AddTopic adds a single topic to the message, along with the corresponding QoS.
// An error is returned if QoS is invalid.
func (msg *SubscribeMessage) AddTopic(topic string, qos QosType) error {
	if !qos.IsValid() {
		return ErrInvalidQoS
	}

	// [MQTT-3.8.3-1]
	if !utf8.Valid([]byte(topic)) {
		return ErrMalformedTopic
	}

	msg.topics.Set(topic, qos)

	return nil
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (msg *SubscribeMessage) RemoveTopic(topic string) {
	msg.topics.Delete(topic)
}

// TopicQos returns the QoS level of a topic. If topic does not exist, QosFailure
// is returned.
//func (msg *SubscribeMessage) TopicQos(topic string) QosType {
//
//	if _, ok := msg.topics[topic]; ok {
//		return msg.topics[topic]
//	}
//
//	return QosFailure
//}

// Qos returns the list of QoS current in the message.
//func (msg *SubscribeMessage) Qos() []QosType {
//	qos := []QosType{}
//
//	for _, q := range msg.topics {
//		qos = append(qos, q)
//	}
//
//	return qos
//}

// SetPacketID sets the ID of the packet.
func (msg *SubscribeMessage) SetPacketID(v uint16) {
	msg.packetID = v
}

// decode message
func (msg *SubscribeMessage) decode(src []byte) (int, error) {
	total := 0

	hn, err := msg.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	msg.packetID = binary.BigEndian.Uint16(src[total:])
	total += 2

	remLen := int(msg.remLen) - (total - hn)
	for remLen > 0 {
		t, n, err := readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		// [MQTT-3.8.3-1]
		if !utf8.Valid(t) {
			return total, ErrMalformedTopic
		}

		qos := QosType(src[total])

		// [MQTT-3-8.3-4]
		if !qos.IsValid() {
			return total, ErrInvalidQoS
		}

		msg.topics.Set(string(t), qos)
		total++

		remLen = remLen - n - 1
	}

	// [MQTT-3.8.3-3]
	if msg.topics.Len() == 0 {
		return 0, errors.New("subscribe/decode: Empty topic list")
	}

	return total, nil
}

func (msg *SubscribeMessage) preEncode(dst []byte) (int, error) {
	// [MQTT-2.3.1]
	if msg.packetID == 0 {
		return 0, ErrPackedIDZero
	}

	var err error
	total := 0

	var n int

	total += msg.header.encode(dst[total:])

	binary.BigEndian.PutUint16(dst[total:], msg.packetID)
	total += 2

	iter := msg.topics.Iterator()
	for kv, ok := iter(); ok; kv, ok = iter() {
		n, err = writeLPBytes(dst[total:], []byte(kv.Key.(string)))
		total += n
		if err != nil {
			return total, err
		}

		dst[total] = byte(kv.Value.(QosType))
		total++
	}

	return total, err
}

// Encode message
func (msg *SubscribeMessage) Encode(dst []byte) (int, error) {
	expectedSize, err := msg.Size()
	if err != nil {
		return 0, err
	}

	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	return msg.preEncode(dst)
}

func (msg *SubscribeMessage) size() int {
	// packet ID
	total := 2

	iter := msg.topics.Iterator()
	for kv, ok := iter(); ok; kv, ok = iter() {
		total += 2 + len(kv.Key.(string)) + 1
	}

	return total
}
