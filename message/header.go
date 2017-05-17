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
	"fmt"
)

// Fixed header
// - 1 byte for control packet type (bits 7-4) and flags (bits 3-0)
// - up to 4 byte for remaining length
type header struct {
	remLen   int32
	packetID uint16
	// mTypeFlags is the first byte of the buffer, 4 bits for mType, 4 bits for flags
	mTypeFlags []byte
}

// String returns a string representation of the message.
func (h header) String() string {
	return fmt.Sprintf("Type=%q, Flags=%08b, Remaining Length=%d", h.Type().Name(), h.Flags(), h.remLen)
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

// Type returns the MessageType of the Message. The retured value should be one
// of the constants defined for MessageType.
func (h *header) Type() Type {
	//return this.mtype
	if len(h.mTypeFlags) != 1 {
		h.mTypeFlags = make([]byte, 1)
	}

	return Type(h.mTypeFlags[0] >> 4)
}

// SetType sets the message type of this message. It also correctly sets the
// default flags for the message type. It returns an error if the type is invalid.
func (h *header) SetType(mType Type) error {
	if !mType.Valid() {
		return ErrInvalidMessageType
	}

	// Notice we don't set the message to be dirty when we are not allocating a new
	// buffer. In this case, it means the buffer is probably a sub-slice of another
	// slice. If that's the case, then during encoding we would have copied the whole
	// backing buffer anyway.
	if len(h.mTypeFlags) != 1 {
		h.mTypeFlags = make([]byte, 1)
	}

	h.mTypeFlags[0] = byte(mType)<<4 | (mType.DefaultFlags() & 0xf)

	return nil
}

// Flags returns the fixed header flags for this message.
func (h *header) Flags() byte {
	return h.mTypeFlags[0] & 0x0F
}

// RemainingLength returns the length of the non-fixed-header part of the message.
func (h *header) RemainingLength() int32 {
	return h.remLen
}

func (h *header) PacketID() uint16 {
	return h.packetID
}

// SetRemainingLength sets the length of the non-fixed-header part of the message.
// It returns error if the length is greater than 268435455, which is the max
// message length as defined by the MQTT spec.
func (h *header) SetRemainingLength(remlen int32) error {
	if remlen > maxRemainingLength || remlen < 0 {
		return ErrInvalidLength
	}

	h.remLen = remlen

	return nil
}

func (h *header) Len() int {
	return h.msgLen()
}

func (h *header) encode(dst []byte) (int, error) {
	ml := h.msgLen()

	if len(dst) < ml {
		return 0, ErrInvalidLength
	}

	total := 0

	if h.remLen > maxRemainingLength || h.remLen < 0 {
		return total, ErrInvalidLength
	}

	if !h.Type().Valid() {
		return total, ErrInvalidMessageType
	}

	copy(dst[total:], h.mTypeFlags)
	total++

	n := binary.PutUvarint(dst[total:], uint64(h.remLen))
	total += n

	return total, nil
}

// Decode reads from the io.Reader parameter until a full message is decoded, or
// when io.Reader returns EOF or error. The first return value is the number of
// bytes read from io.Reader. The second is error if Decode encounters any problems.
func (h *header) decode(src []byte) (int, error) {
	total := 0

	// Decode fixed header
	mType := h.Type()

	h.mTypeFlags = make([]byte, 1)
	copy(h.mTypeFlags, src[total:total+1])
	// [MQTT-2.2.1]
	if !h.Type().Valid() {
		return total, ErrInvalidMessageType
	}

	// This error seems meaningless
	if mType != h.Type() {
		return total, ErrInvalidMessageType
	}

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

	remLen, m := binary.Uvarint(src[total:])
	total += m
	h.remLen = int32(remLen)

	if h.remLen > maxRemainingLength {
		return total, ErrInvalidLength
	}

	//if h.remLen > maxRemainingLength || remLen < 0 {
	//	return total, fmt.Errorf("header/Decode: Remaining length (%d) out of bound (max %d, min 0)", h.remLen, maxRemainingLength)
	//}

	if int(h.remLen) > len(src[total:]) {
		return total, ErrInvalidLength
	}

	return total, nil
}

func (h *header) msgLen() int {
	// message type and flag byte
	total := 1

	if h.remLen <= 127 {
		total++
	} else if h.remLen <= 16383 {
		total += 2
	} else if h.remLen <= 2097151 {
		total += 3
	} else {
		total += 4
	}

	return total
}
