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

	"github.com/troian/surgemq/buffer"
)

// UnSubscribeMessage An UNSUBSCRIBE Packet is sent by the Client to the Server, to unsubscribe from topics.
type UnSubscribeMessage struct {
	header

	topics TopicsQoS
}

var _ Provider = (*UnSubscribeMessage)(nil)

// NewUnSubscribeMessage creates a new UNSUBSCRIBE message.
func NewUnSubscribeMessage() *UnSubscribeMessage {
	msg := &UnSubscribeMessage{
		topics: make(TopicsQoS),
	}
	msg.SetType(UNSUBSCRIBE) // nolint: errcheck

	return msg
}

// Topics returns a list of topics sent by the Client.
func (msg *UnSubscribeMessage) Topics() Topics {
	topics := Topics{}

	for t := range msg.topics {
		topics = append(topics, t)
	}

	return topics
}

// AddTopic adds a single topic to the message.
func (msg *UnSubscribeMessage) AddTopic(topic string) {
	if msg.TopicExists(topic) {
		return
	}

	msg.topics[topic] = 0
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (msg *UnSubscribeMessage) RemoveTopic(topic string) {
	if msg.TopicExists(topic) {
		delete(msg.topics, topic)
	}
}

// TopicExists checks to see if a topic exists in the list.
func (msg *UnSubscribeMessage) TopicExists(topic string) bool {
	if _, ok := msg.topics[topic]; ok {
		return true
	}

	return false
}

// SetPacketID sets the ID of the packet.
func (msg *UnSubscribeMessage) SetPacketID(v uint16) {
	msg.packetID = v
}

// Len of message
func (msg *UnSubscribeMessage) Len() int {
	ml := msg.msgLen()

	if err := msg.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return msg.header.msgLen() + ml
}

// Decode reads from the io.Reader parameter until a full message is decoded, or
// when io.Reader returns EOF or error. The first return value is the number of
// bytes read from io.Reader. The second is error if Decode encounters any problems.
func (msg *UnSubscribeMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := msg.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	msg.packetID = binary.BigEndian.Uint16(src[total:])
	total += 2

	remlen := int(msg.remLen) - (total - hn)
	for remlen > 0 {
		t, n, err := readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		msg.topics[string(t)] = 0
		remlen = remlen - n - 1
	}

	if len(msg.topics) == 0 {
		return 0, errors.New("unsubscribe/Decode: Empty topic list")
	}

	return total, nil
}

func (msg *UnSubscribeMessage) preEncode(dst []byte) (int, error) {
	// [MQTT-2.3.1]
	if msg.packetID == 0 {
		return 0, ErrPackedIDZero
	}

	var err error
	total := 0

	var n int

	if n, err = msg.header.encode(dst[total:]); err != nil {
		return total, err
	}
	total += n

	binary.BigEndian.PutUint16(dst[total:], msg.packetID)
	total += 2

	for t := range msg.topics {
		n, err = writeLPBytes(dst[total:], []byte(t))
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
func (msg *UnSubscribeMessage) Encode(dst []byte) (int, error) {
	expectedSize := msg.Len()
	if len(dst) < expectedSize {
		return expectedSize, ErrInsufficientBufferSize
	}

	return msg.preEncode(dst)
}

// Send encode and send message into ring buffer
func (msg *UnSubscribeMessage) Send(to *buffer.Type) (int, error) {
	expectedSize := msg.Len()
	if len(to.ExternalBuf) < expectedSize {
		to.ExternalBuf = make([]byte, expectedSize)
	}

	total, err := msg.preEncode(to.ExternalBuf)
	if err != nil {
		return 0, err
	}

	return to.Send([][]byte{to.ExternalBuf[:total]})
}

func (msg *UnSubscribeMessage) msgLen() int {
	// packet ID
	total := 2

	for t := range msg.topics {
		total += 2 + len(t)
	}

	return total
}
