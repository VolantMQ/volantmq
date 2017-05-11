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

func (msg *UnSubscribeMessage) String() string {
	msgstr := fmt.Sprintf("%s", msg.header)

	i := 0
	for t := range msg.topics {
		msgstr = fmt.Sprintf("%s, Topic%d=%s", msgstr, i, t)
		i++
	}

	return msgstr
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
	msg.dirty = true
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (msg *UnSubscribeMessage) RemoveTopic(topic string) {
	if msg.TopicExists(topic) {
		delete(msg.topics, topic)
		msg.dirty = true
	}
}

// TopicExists checks to see if a topic exists in the list.
func (msg *UnSubscribeMessage) TopicExists(topic string) bool {
	if _, ok := msg.topics[topic]; ok {
		return true
	}

	return false
}

// Len of message
func (msg *UnSubscribeMessage) Len() int {
	if !msg.dirty {
		return len(msg.dBuf)
	}

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

		msg.topics[string(t)] = 0
		remlen = remlen - n - 1
	}

	if len(msg.topics) == 0 {
		return 0, errors.New("unsubscribe/Decode: Empty topic list")
	}

	msg.dirty = false

	return total, nil
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
			//this.packetId = uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff)
		}
		total += copy(dst[total:], msg.packetID)

		for t := range msg.topics {
			n, err = writeLPBytes(dst[total:], []byte(t))
			total += n
			if err != nil {
				return total, err
			}
		}
	}

	return total, err
}

// Send encode and send message into ring buffer
func (msg *UnSubscribeMessage) Send(to *buffer.Type) (int, error) {
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

		total += copy(to.ExternalBuf[total:total+2], msg.packetID)

		for t := range msg.topics {
			var n int
			n, err = writeLPBytes(to.ExternalBuf[total:], []byte(t))
			total += n
			if err != nil {
				return total, err
			}
		}

		total, err = to.Send(to.ExternalBuf[:total])
	}

	return total, err
}

func (msg *UnSubscribeMessage) msgLen() int {
	// packet ID
	total := 2

	for t := range msg.topics {
		total += 2 + len(t)
	}

	return total
}
