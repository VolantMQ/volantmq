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

package packet

import (
	"unicode/utf8"

	"github.com/troian/omap"
)

// UnSubscribe An UNSUBSCRIBE Packet is sent by the Client to the Server, to unsubscribe from topics.
type UnSubscribe struct {
	header

	topics omap.Map
}

var _ Provider = (*UnSubscribe)(nil)

func newUnSubscribe() *UnSubscribe {
	msg := &UnSubscribe{
		topics: omap.New(),
	}

	return msg
}

// Topics returns a list of topics sent by the Client.
func (msg *UnSubscribe) Topics() omap.Map {
	return msg.topics
}

// AddTopic adds a single topic to the message.
func (msg *UnSubscribe) AddTopic(topic string) error {
	// [MQTT-3.8.3-1]
	if !utf8.Valid([]byte(topic)) {
		return ErrMalformedTopic
	}

	msg.topics.Set(topic, QoS0)

	return nil
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (msg *UnSubscribe) RemoveTopic(topic string) {
	msg.topics.Delete(topic)
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

		msg.topics.Set(string(t), QoS0)

		remLen = remLen - n - 1
	}

	// [MQTT-3.10.3-2]
	if msg.topics.Len() == 0 {
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

	if msg.topics.Len() == 0 {
		return 0, ErrInvalidLength
	}

	var err error
	var n int

	total := msg.encodePacketID(dst)

	iter := msg.topics.Iterator()
	for kv, ok := iter(); ok; kv, ok = iter() {
		n, err = WriteLPBytes(dst[total:], []byte(kv.Key.(string)))
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

	iter := msg.topics.Iterator()
	for kv, ok := iter(); ok; kv, ok = iter() {
		total += 2 + len(kv.Key.(string))
	}

	return total
}
