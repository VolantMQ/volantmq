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

// UnSubscribe An UNSUBSCRIBE Packet is sent by the Client to the Server, to unsubscribe from topics.
type UnSubscribe struct {
	header

	topics []string
}

var _ Provider = (*UnSubscribe)(nil)

func newUnSubscribe() *UnSubscribe {
	return &UnSubscribe{}
}

// NewUnSubscribe creates a new UNSUBSCRIBE packet
func NewUnSubscribe(v ProtocolVersion) *UnSubscribe {
	p := newUnSubscribe()
	p.init(UNSUBSCRIBE, v, p.size, p.encodeMessage, p.decodeMessage)
	return p
}

// Topics returns a list of topics sent by the Client.
func (msg *UnSubscribe) Topics() []string {
	return msg.topics
}

// AddTopic adds a single topic to the message.
func (msg *UnSubscribe) AddTopic(topic string) error {
	// [MQTT-3.8.3-1]
	if !utf8.Valid([]byte(topic)) {
		return ErrMalformedTopic
	}

	msg.topics = append(msg.topics, topic)

	return nil
}

// SetPacketID sets the ID of the packet.
func (msg *UnSubscribe) SetPacketID(v IDType) {
	msg.setPacketID(v)
}

// decode reads from the io.Reader parameter until a full message is decoded, or
// when io.Reader returns EOF or error. The first return value is the number of
// bytes read from io.Reader. The second is error if decode encounters any problems.
func (msg *UnSubscribe) decodeMessage(src []byte) (int, error) {
	total := msg.decodePacketID(src)

	remLen := int(msg.remLen) - total
	for remLen > 0 {
		t, n, err := ReadLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		// [MQTT-3.10.3-1]
		if !utf8.Valid(t) {
			return total, ErrMalformedTopic
		}

		msg.topics = append(msg.topics, string(t))

		remLen = remLen - n - 1
	}

	// [MQTT-3.10.3-2]
	if len(msg.topics) == 0 {
		rejectReason := CodeProtocolError
		if msg.version <= ProtocolV50 {
			rejectReason = CodeRefusedServerUnavailable
		}
		return 0, rejectReason
	}

	return total, nil
}

func (msg *UnSubscribe) encodeMessage(dst []byte) (int, error) {
	// [MQTT-2.3.1]
	if len(msg.packetID) == 0 {
		return 0, ErrPackedIDZero
	}

	if len(msg.topics) == 0 {
		return 0, ErrInvalidLength
	}

	var err error
	var n int

	total := msg.encodePacketID(dst)

	for _, t := range msg.topics {
		n, err = WriteLPBytes(dst[total:], []byte(t))
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, err
}

// Encode returns an io.Reader in which the encoded bytes can be read. The second
// return value is the number of bytes encoded, so the caller knows how many bytes
// there will be. If Encode returns an error, then the first two return values
// should be considered invalid.
// Any changes to the message after Encode() is called will invalidate the io.Reader.

func (msg *UnSubscribe) size() int {
	// packet ID
	total := 2

	for _, t := range msg.topics {
		total += 2 + len(t)
	}

	return total
}
