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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageHeaderFields(t *testing.T) {
	hdr := &header{}

	hdr.setRemainingLength(33) // nolint: errcheck

	require.Equal(t, int32(33), hdr.RemainingLength())

	err := hdr.setRemainingLength(268435456)

	require.Error(t, err)

	err = hdr.setRemainingLength(-1)

	require.Error(t, err)

	hdr.setType(PUBREL)

	require.Equal(t, PUBREL, hdr.Type())
	require.Equal(t, "PUBREL", hdr.Name())
	require.Equal(t, 2, int(hdr.Flags()))
	require.Equal(t, PUBREL.Desc(), hdr.Desc())
}

// Not enough bytes
func TestMessageHeaderDecode(t *testing.T) {
	buf := []byte{0x6f, 193, 2}
	hdr := &header{}

	_, err := hdr.decode(buf)
	require.Error(t, err)
}

// Remaining length too big
func TestMessageHeaderDecode2(t *testing.T) {
	buf := []byte{0x62, 0xff, 0xff, 0xff, 0xff}
	hdr := &header{}

	_, err := hdr.decode(buf)
	require.EqualError(t, err, ErrInsufficientDataSize.Error())
}

func TestMessageHeaderDecode3(t *testing.T) {
	buf := []byte{0x62, 0xff}
	hdr := &header{}

	_, err := hdr.decode(buf)
	require.Error(t, err)
}

func TestMessageHeaderDecode4(t *testing.T) {
	buf := []byte{0x62, 0xff, 0xff, 0xff, 0x7f}
	hdr := &header{
		mType:  PUBREL,
		mFlags: 2,
	}

	_, err := hdr.decode(buf)

	require.EqualError(t, ErrInsufficientDataSize, err.Error())
	require.Equal(t, maxRemainingLength, hdr.RemainingLength())
}

func TestMessageHeaderDecode5(t *testing.T) {
	buf := []byte{0x62, 0xff, 0x7f}
	hdr := &header{
		mType:  PUBREL,
		mFlags: 2,
	}

	_, err := hdr.decode(buf)
	require.Error(t, err)
}

func TestMessageHeaderDecode6(t *testing.T) {
	buf := []byte{byte(PUBLISH<<offsetPacketType | 3<<1), 0xff, 0x7f}

	// PUBLISH with invalid QoS value
	hdr := &header{
		mType:  Type(buf[0] >> offsetPacketType),
		mFlags: buf[0] | maskMessageFlags,
	}

	_, err := hdr.decode(buf)
	require.EqualError(t, err, CodeRefusedServerUnavailable.Error())
}

func TestMessageHeaderEncode1(t *testing.T) {
	hdr := &header{}
	//headerBytes := []byte{0x62, 193, 2}

	//hdr.setVT(ProtocolV311, PUBREL)

	//require.NoError(t, err)

	err := hdr.setRemainingLength(321)

	require.NoError(t, err)

	//buf := make([]byte, 3)
	//n, err := hdr.encode(buf)

	//require.NoError(t, err)
	//require.Equal(t, 3, n)
	//require.Equal(t, headerBytes, buf)
}

func TestMessageHeaderEncode2(t *testing.T) {
	hdr := &header{}

	//header.setVT(ProtocolV311, PUBREL)
	//require.NoError(t, err)

	hdr.remLen = 268435456

	//buf := make([]byte, 5)
	//_, err = hdr.encode(buf)
	//
	//require.Error(t, err)
}

func TestMessageHeaderEncode3(t *testing.T) {
	hdr := &header{}
	//headerBytes := []byte{0x62, 0xff, 0xff, 0xff, 0x7f}

	//hdr.setVT(ProtocolV311, PUBREL)

	//require.NoError(t, err)

	err := hdr.setRemainingLength(maxRemainingLength)

	require.NoError(t, err)

	//buf := make([]byte, 5)
	//n, err := hdr.encode(buf)
	//
	//require.NoError(t, err)
	//require.Equal(t, 5, n)
	//require.Equal(t, headerBytes, buf)
}

func TestMessageHeaderUvariantOverflow(t *testing.T) {
	buf := []byte{0xff, 0xff, 0xff, 0xff, 0x7f}

	val, c := uvarint(buf[:4])

	require.Equal(t, uint32(0), val, "Should return 0 on small buf")
	require.Equal(t, 0, c, "Should return 0 on small buf")

	val, c = uvarint(buf)

	require.Equal(t, uint32(0), val, "Should return 0 on overflow")
	require.Equal(t, -5, c, "Should return overflow count")
}

func TestMessageHeaderEncode4(t *testing.T) {
	type testMessage struct {
		header
	}

	var msg testMessage
	//msg.setVT(ProtocolV311, PUBLISH)
	//require.NoError(t, err)

	msg.cb.size = func() int {
		return 0xFFFFFFF1
	}

	sz, err := msg.Size()
	require.Equal(t, 0, sz)
	require.EqualError(t, ErrInvalidLength, err.Error())
}
