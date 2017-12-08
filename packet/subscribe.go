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
)

// Subscribe The SUBSCRIBE Packet is sent from the Client to the Server to create one or more
// Subscriptions. Each Subscription registers a Client’s interest in one or more
// Topics. The Server sends PUBLISH Packets to the Client in order to forward
// Application Messages that were published to Topics that match these Subscriptions.
// The SUBSCRIBE Packet also specifies (for each Subscription) the maximum QoS with
// which the Server can send Application Messages to the Client.
type Subscribe struct {
	header
	topics []string
	ops    []SubscriptionOptions
}

var _ Provider = (*Subscribe)(nil)

func newSubscribe() *Subscribe {
	return &Subscribe{}
}

// NewSubscribe creates a new SUBSCRIBE packet
func NewSubscribe(v ProtocolVersion) *Subscribe {
	p := newSubscribe()
	p.init(SUBSCRIBE, v, p.size, p.encodeMessage, p.decodeMessage)
	return p
}

// RangeTopics loop through list of topics
func (msg *Subscribe) RangeTopics(fn func(string, SubscriptionOptions)) {
	for i, t := range msg.topics {
		fn(t, msg.ops[i])
	}
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

	msg.topics = append(msg.topics, topic)
	msg.ops = append(msg.ops, ops)

	return nil
}

// SetPacketID sets the ID of the packet.
func (msg *Subscribe) SetPacketID(v IDType) {
	msg.setPacketID(v)
}

// decode message
func (msg *Subscribe) decodeMessage(from []byte) (int, error) {
	offset := msg.decodePacketID(from)

	// v5 [MQTT-3.1.2.11] specifies properties in variable header
	if msg.version == ProtocolV50 {
		n, err := msg.properties.decode(msg.Type(), from[offset:])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	remLen := int(msg.remLen) - offset
	for remLen > 0 {
		t, n, err := ReadLPBytes(from[offset:])
		offset += n
		if err != nil {
			return offset, err
		}

		// [MQTT-3.8.3-1]
		if !utf8.Valid(t) {
			rejectReason := CodeProtocolError
			if msg.version <= ProtocolV50 {
				rejectReason = CodeRefusedServerUnavailable
			}
			return 0, rejectReason
		}

		subsOptions := SubscriptionOptions(from[offset])
		offset++

		if msg.version == ProtocolV50 && (byte(subsOptions)&maskSubscriptionReserved) != 0 {
			return offset, CodeProtocolError
		}

		// [MQTT-3-8.3-4]
		if !subsOptions.QoS().IsValid() {
			rejectReason := CodeProtocolError
			if msg.version <= ProtocolV50 {
				rejectReason = CodeRefusedServerUnavailable
			}
			return offset, rejectReason
		}

		msg.topics = append(msg.topics, string(t))
		msg.ops = append(msg.ops, subsOptions)

		remLen = remLen - n - 1
	}

	// [MQTT-3.8.3-3]
	if len(msg.topics) == 0 {
		rejectReason := CodeProtocolError
		if msg.version <= ProtocolV50 {
			rejectReason = CodeRefusedServerUnavailable
		}
		return 0, rejectReason
	}

	return offset, nil
}

func (msg *Subscribe) encodeMessage(to []byte) (int, error) {
	// [MQTT-2.3.1]
	if len(msg.packetID) == 0 {
		return 0, ErrPackedIDZero
	}

	offset := msg.encodePacketID(to)

	// V5.0   [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		n, err := msg.properties.encode(to[offset:])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	for i, t := range msg.topics {
		n, err := WriteLPBytes(to[offset:], []byte(t))
		offset += n
		if err != nil {
			return offset, err
		}

		to[offset] = byte(msg.ops[i])
		offset++
	}

	return offset, nil
}

func (msg *Subscribe) size() int {
	// packet ID
	total := 2

	// v5.0 [MQTT-3.1.2.11]
	if msg.version == ProtocolV50 {
		total += int(msg.properties.FullLen())
	}

	for _, t := range msg.topics {
		total += 2 + len(t) + 1
	}

	return total
}
