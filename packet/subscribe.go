// Copyright (c) 2014 The VolantMQ Authors. All rights reserved.
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

package packet

import (
	"unicode/utf8"

	"github.com/troian/omap"
)

// Subscribe The SUBSCRIBE Packet is sent from the Client to the Server to create one or more
// Subscriptions. Each Subscription registers a Client’s interest in one or more
// Topics. The Server sends PUBLISH Packets to the Client in order to forward
// Application Messages that were published to Topics that match these Subscriptions.
// The SUBSCRIBE Packet also specifies (for each Subscription) the maximum QoS with
// which the Server can send Application Messages to the Client.
type Subscribe struct {
	header

	topics omap.Map
}

var _ Provider = (*Subscribe)(nil)

func newSubscribe() *Subscribe {
	msg := &Subscribe{
		topics: omap.New(),
	}

	return msg
}

// Topics returns a list of topics sent by the Client.
func (msg *Subscribe) Topics() omap.Map {
	return msg.topics
}

// AddTopic adds a single topic to the message, along with the corresponding QoS.
// An error is returned if QoS is invalid.
func (msg *Subscribe) AddTopic(topic string, ops SubscriptionOptions) error {
	if msg.version == ProtocolV50 {
		if byte(ops)&maskSubscriptionReserved != 0 {
			return ErrInvalidArgs
		}
	} else {
		if !QosType(ops).IsValid() {
			return ErrInvalidQoS
		}
	}

	// [MQTT-3.8.3-1]
	if !utf8.Valid([]byte(topic)) {
		return ErrMalformedTopic
	}

	msg.topics.Set(topic, ops)

	return nil
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (msg *Subscribe) RemoveTopic(topic string) {
	msg.topics.Delete(topic)
}

// SetPacketID sets the ID of the packet.
func (msg *Subscribe) SetPacketID(v IDType) {
	msg.setPacketID(v)
}

// decode message
func (msg *Subscribe) decodeMessage(src []byte) (int, error) {
	total := 0

	total += msg.decodePacketID(src[total:])

	// v5 [MQTT-3.1.2.11] specifies properties in variable header
	if msg.version == ProtocolV50 {
		var n int
		var err error

		if msg.properties, n, err = decodeProperties(msg.mType, src[total:]); err != nil {
			return total + n, err
		}
		total += n
	}

	remLen := int(msg.remLen) - total
	for remLen > 0 {
		t, n, err := ReadLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		// [MQTT-3.8.3-1]
		if !utf8.Valid(t) {
			rejectReason := CodeProtocolError
			if msg.version <= ProtocolV50 {
				rejectReason = CodeRefusedServerUnavailable
			}
			return 0, rejectReason
		}

		subsOptions := SubscriptionOptions(src[total])

		if msg.version == ProtocolV50 && (byte(subsOptions)&maskSubscriptionReserved) != 0 {
			return total, CodeProtocolError
		}

		// [MQTT-3-8.3-4]
		if !subsOptions.QoS().IsValid() {
			rejectReason := CodeProtocolError
			if msg.version <= ProtocolV50 {
				rejectReason = CodeRefusedServerUnavailable
			}
			return 0, rejectReason
		}

		msg.topics.Set(string(t), subsOptions)
		total++

		remLen = remLen - n - 1
	}

	// [MQTT-3.8.3-3]
	if msg.topics.Len() == 0 {
		rejectReason := CodeProtocolError
		if msg.version <= ProtocolV50 {
			rejectReason = CodeRefusedServerUnavailable
		}
		return 0, rejectReason
	}

	return total, nil
}

func (msg *Subscribe) encodeMessage(dst []byte) (int, error) {
	// [MQTT-2.3.1]
	if len(msg.packetID) == 0 {
		return 0, ErrPackedIDZero
	}

	var err error
	total := 0

	var n int

	total += msg.encodePacketID(dst[total:])

	// V5.0   [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		if n, err = encodeProperties(msg.properties, dst[total:]); err != nil {
			return total + n, err
		}

		total += n
	}

	iter := msg.topics.Iterator()
	for kv, ok := iter(); ok; kv, ok = iter() {
		n, err = WriteLPBytes(dst[total:], []byte(kv.Key.(string)))
		total += n
		if err != nil {
			return total, err
		}

		dst[total] = byte(kv.Value.(SubscriptionOptions))
		total++
	}

	return total, err
}

func (msg *Subscribe) size() int {
	// packet ID
	total := 2

	// v5.0 [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		pLen, _ := encodeProperties(msg.properties, []byte{})
		total += pLen
	}

	iter := msg.topics.Iterator()
	for kv, ok := iter(); ok; kv, ok = iter() {
		total += 2 + len(kv.Key.(string)) + 1
	}

	return total
}
