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
	"sync/atomic"
)

// UnSubscribeMessage An UNSUBSCRIBE Packet is sent by the Client to the Server, to unsubscribe from topics.
type UnSubscribeMessage struct {
	header

	topics TopicsQoS
}

var _ Message = (*UnSubscribeMessage)(nil)

// NewUnSubscribeMessage creates a new UNSUBSCRIBE message.
func NewUnSubscribeMessage() *UnSubscribeMessage {
	msg := &UnSubscribeMessage{
		topics: make(TopicsQoS),
	}
	msg.SetType(UNSUBSCRIBE) // nolint: errcheck

	return msg
}

func (usm UnSubscribeMessage) String() string {
	msgstr := fmt.Sprintf("%s", usm.header)

	i := 0
	for t := range usm.topics {
		msgstr = fmt.Sprintf("%s, Topic%d=%s", msgstr, i, t)
		i++
	}

	return msgstr
}

// Topics returns a list of topics sent by the Client.
func (usm *UnSubscribeMessage) Topics() Topics {
	topics := Topics{}

	for t := range usm.topics {
		topics = append(topics, t)
	}

	return topics
}

// AddTopic adds a single topic to the message.
func (usm *UnSubscribeMessage) AddTopic(topic string) {
	if usm.TopicExists(topic) {
		return
	}

	usm.topics[topic] = 0
	usm.dirty = true
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (usm *UnSubscribeMessage) RemoveTopic(topic string) {
	if usm.TopicExists(topic) {
		delete(usm.topics, topic)
		usm.dirty = true
	}
}

// TopicExists checks to see if a topic exists in the list.
func (usm *UnSubscribeMessage) TopicExists(topic string) bool {
	if _, ok := usm.topics[topic]; ok {
		return true
	}

	return false
}

// Len of message
func (usm *UnSubscribeMessage) Len() int {
	if !usm.dirty {
		return len(usm.dBuf)
	}

	ml := usm.msgLen()

	if err := usm.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return usm.header.msgLen() + ml
}

// Decode reads from the io.Reader parameter until a full message is decoded, or
// when io.Reader returns EOF or error. The first return value is the number of
// bytes read from io.Reader. The second is error if Decode encounters any problems.
func (usm *UnSubscribeMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := usm.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	//this.packetId = binary.BigEndian.Uint16(src[total:])
	usm.packetID = src[total : total+2]
	total += 2

	remlen := int(usm.remLen) - (total - hn)
	for remlen > 0 {
		t, n, err := readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		usm.topics[string(t)] = 0
		remlen = remlen - n - 1
	}

	if len(usm.topics) == 0 {
		return 0, errors.New("unsubscribe/Decode: Empty topic list")
	}

	usm.dirty = false

	return total, nil
}

// Encode returns an io.Reader in which the encoded bytes can be read. The second
// return value is the number of bytes encoded, so the caller knows how many bytes
// there will be. If Encode returns an error, then the first two return values
// should be considered invalid.
// Any changes to the message after Encode() is called will invalidate the io.Reader.
func (usm *UnSubscribeMessage) Encode(dst []byte) (int, error) {
	if !usm.dirty {
		if len(dst) < len(usm.dBuf) {
			return 0, fmt.Errorf("unsubscribe/Encode: Insufficient buffer size. Expecting %d, got %d", len(usm.dBuf), len(dst))
		}

		return copy(dst, usm.dBuf), nil
	}

	hl := usm.header.msgLen()
	ml := usm.msgLen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("unsubscribe/Encode: Insufficient buffer size. Expecting %d, got %d", hl+ml, len(dst))
	}

	if err := usm.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := usm.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	if usm.PacketID() == 0 {
		usm.SetPacketID(uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff))
		//this.packetId = uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff)
	}

	n = copy(dst[total:], usm.packetID)
	//binary.BigEndian.PutUint16(dst[total:], this.packetId)
	total += n

	for t := range usm.topics {
		n, err := writeLPBytes(dst[total:], []byte(t))
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (usm *UnSubscribeMessage) msgLen() int {
	// packet ID
	total := 2

	for t := range usm.topics {
		total += 2 + len(t)
	}

	return total
}
