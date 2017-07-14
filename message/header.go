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
)

type sizeCallback func() int

// Fixed header
// - 1 byte for control packet type (bits 7-4) and flags (bits 3-0)
// - up to 4 byte for remaining length
type header struct {
	remLen     int32
	packetID   uint16
	mTypeFlags byte // is the first byte of the buffer, 4 bits for mType, 4 bits for flags
	sizeCb     sizeCallback
}

// Name returns a string representation of the message type. Examples include
// "PUBLISH", "SUBSCRIBE", and others. This is statically defined for each of
// the message types and cannot be changed.
func (h *header) Name() string {
	return h.Type().Name()
}

// Desc returns a string description of the message type. For example, a
// CONNECT message would return "Client request to connect to Server." These
// descriptions are statically defined (copied from the MQTT spec) and cannot
// be changed.
func (h *header) Desc() string {
	return h.Type().Desc()
}

// Type returns the MessageType of the Message
func (h *header) Type() Type {
	return Type(h.mTypeFlags >> 4)
}

// Flags returns the fixed header flags for this message.
func (h *header) Flags() byte {
	return h.mTypeFlags & 0x0F
}

// RemainingLength returns the length of the non-fixed-header part of the message.
func (h *header) RemainingLength() int32 {
	return h.remLen
}

func (h *header) PacketID() uint16 {
	return h.packetID
}

// Size of message
func (h *header) Size() (int, error) {
	ml := h.sizeCb()

	if err := h.setRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	return h.size() + ml, nil
}

// setType sets the message type of this message. It also correctly sets the
// default flags for the message type. It returns an error if the type is invalid.
func (h *header) setType(t Type) error {
	if !t.Valid() {
		return ErrInvalidMessageType
	}

	// Notice we don't set the message to be dirty when we are not allocating a new
	// buffer. In this case, it means the buffer is probably a sub-slice of another
	// slice. If that's the case, then during encoding we would have copied the whole
	// backing buffer anyway.
	h.mTypeFlags = byte(t)<<4 | (t.DefaultFlags() & 0xf)

	return nil
}

// setRemainingLength sets the length of the non-fixed-header part of the message.
// It returns error if the length is greater than 268435455, which is the max
// message length as defined by the MQTT spec.
func (h *header) setRemainingLength(remLen int32) error {
	if remLen > maxRemainingLength || remLen < 0 {
		return ErrInvalidLength
	}

	h.remLen = remLen

	return nil
}

// encode fixed header and remaining length into buffer
// each message type must provide dst buffer with appropriate size
// provided by header.Size()
func (h *header) encode(dst []byte) int {
	total := 0

	dst[total] = h.mTypeFlags
	total++

	n := binary.PutUvarint(dst[total:], uint64(h.remLen))
	total += n

	return total
}

// decode reads fixed header and remaining length
// if decode successful size of decoded data provided
// if error happened offset points to error place
func (h *header) decode(src []byte) (int, error) {
	total := 0

	// decode and validate fixed header
	h.mTypeFlags = src[total]

	// [MQTT-2.2.2-1]
	if h.Type() != PUBLISH {
		if h.Flags() != h.Type().DefaultFlags() {
			return total, ErrInvalidMessageTypeFlags
		}
	} else {
		if !QosType((h.Flags() & publishFlagQosMask) >> 1).IsValid() {
			return total, ErrInvalidQoS
		}
	}

	total++

	remLen, m := uvarint(src[total:])
	if m == 0 {
		return total, ErrInvalidLength
	}

	total += m
	h.remLen = int32(remLen)

	// we don't check remaining length max here as uvariant will return count 0 in case of
	// returned value bigger than 256MB

	//if h.remLen > maxRemainingLength {
	//	return total, ErrInvalidLength
	//}

	// verify if buffer has enough space for whole message
	// if not return expected size
	if int(h.remLen) > len(src[total:]) {
		return total + int(h.remLen), ErrInsufficientBufferSize
	}

	return total, nil
}

// size of header
// this function must be invoked after successful call to setRemainingLength
func (h *header) size() int {
	// message type and flags byte
	total := 1

	return total + uvarintCalc(uint32(h.remLen))
	//if h.remLen <= 127 {
	//	total++
	//} else if h.remLen <= 16383 {
	//	total += 2
	//} else if h.remLen <= 2097151 {
	//	total += 3
	//} else {
	//	total += 4
	//}

	//return total
}

// uvarint decodes a uint64 from buf and returns that value and the
// number of bytes read (> 0). If an error occurred, the value is 0
// and the number of bytes n is <= 0 meaning:
//
//	n == 0: buf too small
//	n  < 0: value larger than 32 bits (overflow)
//              and -n is the number of bytes read
//
// copied from binary.Uvariant
func uvarint(buf []byte) (uint32, int) {
	var x uint32
	var s uint
	for i, b := range buf {
		if b < 0x80 {
			if i > 4 || i == 4 && b > 1 {
				return 0, -(i + 1) // overflow
			}
			return x | uint32(b)<<s, i + 1
		}
		x |= uint32(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

func uvarintCalc(x uint32) int {
	i := 0
	for x >= 0x80 {
		//buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	//buf[i] = byte(x)
	return i + 1
}
