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

package sessions

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/troian/surgemq/message"
)

var (
	//errQueueFull   error = errors.New("queue full")
	errQueueEmpty  error = errors.New("queue empty")
	errWaitMessage error = errors.New("Invalid message to wait for ack")
	errAckMessage  error = errors.New("Invalid message for acking")
)

// AckMsg message
type AckMsg struct {
	// Message type of the message waiting for ack
	MType message.MessageType

	// Current state of the ack-waiting message
	State message.MessageType

	// Packet ID of the message. Every message that require ack'ing must have a valid
	// packet ID. Messages that have message I
	PktID uint16

	// Slice containing the message bytes
	MsgBuf []byte

	// Slice containing the ack message bytes
	AckBuf []byte

	// When ack cycle completes, call this function
	OnComplete interface{}
}

// AckQueue is a growing queue implemented based on a ring buffer. As the buffer
// gets full, it will auto-grow.
//
// AckQueue is used to store messages that are waiting for acks to come back. There
// are a few scenarios in which acks are required.
//   1. Client sends SUBSCRIBE message to server, waits for SUBACK.
//   2. Client sends UNSUBSCRIBE message to server, waits for UNSUBACK.
//   3. Client sends PUBLISH QoS 1 message to server, waits for PUBACK.
//   4. Server sends PUBLISH QoS 1 message to client, waits for PUBACK.
//   5. Client sends PUBLISH QoS 2 message to server, waits for PUBREC.
//   6. Server sends PUBREC message to client, waits for PUBREL.
//   7. Client sends PUBREL message to server, waits for PUBCOMP.
//   8. Server sends PUBLISH QoS 2 message to client, waits for PUBREC.
//   9. Client sends PUBREC message to server, waits for PUBREL.
//   10. Server sends PUBREL message to client, waits for PUBCOMP.
//   11. Client sends PINGREQ message to server, waits for PINGRESP.
type AckQueue struct {
	size  int64
	mask  int64
	count int64
	head  int64
	tail  int64

	ping AckMsg
	ring []AckMsg
	eMap map[uint16]int64

	ackDone []AckMsg

	mu sync.Mutex
}

func newAckQueue(n int) *AckQueue {
	m := int64(n)
	if !powerOfTwo64(m) {
		m = roundUpPowerOfTwo64(m)
	}

	return &AckQueue{
		size:    m,
		mask:    m - 1,
		count:   0,
		head:    0,
		tail:    0,
		ring:    make([]AckMsg, m),
		eMap:    make(map[uint16]int64, m),
		ackDone: make([]AckMsg, 0),
	}
}

// Wait copies the message into a waiting queue, and waits for the corresponding
// ack message to be received.
func (a *AckQueue) Wait(msg message.Message, onComplete interface{}) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch msg := msg.(type) {
	case *message.PublishMessage:
		if msg.QoS() == message.QosAtMostOnce {
			//return fmt.Errorf("QoS 0 messages don't require ack")
			return errWaitMessage
		}

		a.insert(msg.PacketId(), msg, onComplete)

	case *message.SubscribeMessage:
		a.insert(msg.PacketId(), msg, onComplete)

	case *message.UnSubscribeMessage:
		a.insert(msg.PacketId(), msg, onComplete)

	case *message.PingReqMessage:
		a.ping = AckMsg{
			MType:      message.PINGREQ,
			State:      message.RESERVED,
			OnComplete: onComplete,
		}

	default:
		return errWaitMessage
	}

	return nil
}

// Ack takes the ack message supplied and updates the status of messages waiting.
func (a *AckQueue) Ack(msg message.Message) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch msg.Type() {
	case message.PUBACK, message.PUBREC, message.PUBREL, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
		// Check to see if the message w/ the same packet ID is in the queue
		i, ok := a.eMap[msg.PacketId()]
		if ok {
			// If message w/ the packet ID exists, update the message state and copy
			// the ack message
			a.ring[i].State = msg.Type()

			ml := msg.Len()
			a.ring[i].AckBuf = make([]byte, ml)

			_, err := msg.Encode(a.ring[i].AckBuf)
			if err != nil {
				return err
			}
			//glog.Debugf("Acked: %v", msg)
			//} else {
			//glog.Debugf("Cannot ack %s message with packet ID %d", msg.Type(), msg.PacketId())
		}

	case message.PINGRESP:
		if a.ping.MType == message.PINGREQ {
			a.ping.State = message.PINGRESP
		}

	default:
		return errAckMessage
	}

	return nil
}

// Acked returns the list of messages that have completed the ack cycle.
func (a *AckQueue) GetAckMsg() []AckMsg {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.ackDone = a.ackDone[0:0]

	if a.ping.State == message.PINGRESP {
		a.ackDone = append(a.ackDone, a.ping)
		a.ping = AckMsg{}
	}

FORNOTEMPTY:
	for !a.empty() {
		switch a.ring[a.head].State {
		case message.PUBACK, message.PUBREL, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
			a.ackDone = append(a.ackDone, a.ring[a.head])
			a.removeHead()

		default:
			break FORNOTEMPTY
		}
	}

	return a.ackDone
}

func (a *AckQueue) insert(pktid uint16, msg message.Message, onComplete interface{}) error {
	if a.full() {
		a.grow()
	}

	if _, ok := a.eMap[pktid]; !ok {
		// message length
		ml := msg.Len()

		// ackmsg
		am := AckMsg{
			MType:      msg.Type(),
			State:      message.RESERVED,
			PktID:      msg.PacketId(),
			MsgBuf:     make([]byte, ml),
			OnComplete: onComplete,
		}

		if _, err := msg.Encode(am.MsgBuf); err != nil {
			return err
		}

		a.ring[a.tail] = am
		a.eMap[pktid] = a.tail
		a.tail = a.increment(a.tail)
		a.count++
	} else {
		// If packet w/ pktid already exist, then this must be a PUBLISH message
		// Other message types should never send with the same packet ID
		pm, ok := msg.(*message.PublishMessage)
		if !ok {
			return fmt.Errorf("ack/insert: duplicate packet ID for %s message", msg.Name())
		}

		// If this is a publish message, then the DUP flag must be set. This is the
		// only scenario in which we will receive duplicate messages.
		if pm.Dup() {
			return errors.New("ack/insert: duplicate packet ID for PUBLISH message, but DUP flag is not set")
		}

		// Since it's a dup, there's really nothing we need to do. Moving on...
	}

	return nil
}

func (a *AckQueue) removeHead() error {
	if a.empty() {
		return errQueueEmpty
	}

	it := a.ring[a.head]
	// set this to empty ackmsg{} to ensure GC will collect the buffer
	a.ring[a.head] = AckMsg{}
	a.head = a.increment(a.head)
	a.count--
	delete(a.eMap, it.PktID)

	return nil
}

func (a *AckQueue) grow() {
	if math.MaxInt64/2 < a.size {
		panic("new size will overflow int64")
	}

	newSize := a.size << 1
	newMask := newSize - 1
	newRing := make([]AckMsg, newSize)

	if a.tail > a.head {
		copy(newRing, a.ring[a.head:a.tail])
	} else {
		copy(newRing, a.ring[a.head:])
		copy(newRing[a.size-a.head:], a.ring[:a.tail])
	}

	a.size = newSize
	a.mask = newMask
	a.ring = newRing
	a.head = 0
	a.tail = a.count

	a.eMap = make(map[uint16]int64, a.size)

	for i := int64(0); i < a.tail; i++ {
		a.eMap[a.ring[i].PktID] = i
	}
}

func (a *AckQueue) len() int {
	return int(a.count)
}

func (a *AckQueue) cap() int {
	return int(a.size)
}

func (a *AckQueue) index(n int64) int64 {
	return n & a.mask
}

func (a *AckQueue) full() bool {
	return a.count == a.size
}

func (a *AckQueue) empty() bool {
	return a.count == 0
}

func (a *AckQueue) increment(n int64) int64 {
	return a.index(n + 1)
}

func powerOfTwo64(n int64) bool {
	return n != 0 && (n&(n-1)) == 0
}

func roundUpPowerOfTwo64(n int64) int64 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}
