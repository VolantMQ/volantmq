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

func newTestConnect(t *testing.T, p ProtocolVersion) *Connect {
	m, err := New(p, CONNECT)
	require.NoError(t, err)
	msg, ok := m.(*Connect)
	require.True(t, ok, "Couldn't cast message type")

	return msg
}

func TestConnectMessageFields(t *testing.T) {
	msg := newTestConnect(t, ProtocolV31)

	require.Equal(t, ProtocolV31, msg.Version(), "Incorrect version number")

	msg.SetClean(true)
	require.True(t, msg.IsClean(), "Error setting clean session flag")

	msg.SetClean(false)
	require.False(t, msg.IsClean(), "Error setting clean session flag")

	err := msg.SetWill("topic", []byte("message"), QoS1, true)
	require.NoError(t, err)

	willTopic, willMessage, willQoS, willRetain, will := msg.Will()
	require.True(t, will, "Error setting will flag")
	require.True(t, willRetain, "Error setting will retain")
	require.Equal(t, "topic", willTopic, "Error setting will topic")
	require.Equal(t, []byte("message"), willMessage, "Error setting will topic")
	require.Equal(t, QoS1, willQoS, "Error setting will topic")

	user, pass := msg.Credentials()
	require.Equal(t, 0, len(user))
	require.Equal(t, 0, len(pass))

	err = msg.SetCredentials([]byte("user"), []byte("pass"))
	require.NoError(t, err)

	err = msg.SetClientID([]byte("j0j0jfajf02j0asdjf"))
	require.NoError(t, err, "Error setting client ID")

	require.Equal(t, "j0j0jfajf02j0asdjf", string(msg.ClientID()), "Error setting client ID.")

	err = msg.SetClientID([]byte("this is good for v3"))
	require.NoError(t, err)

	//msg.SetVersion(0x4) // nolint: errcheck
	//
	//err = msg.SetClientID([]byte("this is no good for v4!"))
	//require.Error(t, err)
	//
	//msg.SetVersion(0x3) // nolint: errcheck
	//
	//msg.SetWillTopic("willtopic")
	//require.Equal(t, "willtopic", string(msg.WillTopic()), "Error setting will topic.")
	//
	//require.True(t, msg.WillFlag(), "Error setting will flag.")
	//
	//msg.SetWillTopic("")
	//require.Equal(t, "", string(msg.WillTopic()), "Error setting will topic.")
	//
	//require.False(t, msg.WillFlag(), "Error setting will flag.")
	//
	//msg.SetWillMessage([]byte("this is a will message"))
	//require.Equal(t, "this is a will message", string(msg.WillMessage()), "Error setting will message.")
	//
	//require.True(t, msg.WillFlag(), "Error setting will flag.")
	//
	//msg.SetWillMessage([]byte(""))
	//require.Equal(t, "", string(msg.WillMessage()), "Error setting will topic.")
	//
	//require.False(t, msg.WillFlag(), "Error setting will flag.")
	//
	//msg.SetWillTopic("willtopic")
	//msg.SetWillMessage([]byte("this is a will message"))
	//msg.SetWillTopic("")
	//require.True(t, msg.WillFlag(), "Error setting will topic.")
	//
	//msg.SetUsername([]byte("myname"))
	//require.Equal(t, "myname", string(msg.Username()), "Error setting will message.")
	//
	//require.True(t, msg.UsernameFlag(), "Error setting will flag.")
	//
	//msg.SetUsername([]byte(""))
	//require.Equal(t, "", string(msg.Username()), "Error setting will message.")
	//
	//require.False(t, msg.UsernameFlag(), "Error setting will flag.")
	//
	//msg.SetPassword([]byte("myname"))
	//require.Equal(t, "myname", string(msg.Password()), "Error setting will message.")
	//
	//require.True(t, msg.PasswordFlag(), "Error setting will flag.")
	//
	//msg.SetPassword([]byte(""))
	//require.Equal(t, "", string(msg.Password()), "Error setting will message.")
	//
	//require.False(t, msg.PasswordFlag(), "Error setting will flag.")
}

func TestConnectMessageDecodeV3(t *testing.T) {
	buf := []byte{
		byte(CONNECT << 4),
		62,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		8,   // Client ID LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		8, // Username ID LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	m, n, err := Decode(ProtocolV311, buf)
	require.NoError(t, err, "Error decoding message")
	msg, ok := m.(*Connect)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, 206, int(msg.connectFlags), "Incorrect flag value.")
	require.Equal(t, 10, int(msg.KeepAlive()), "Incorrect KeepAlive value.")
	require.Equal(t, "volantmq", string(msg.ClientID()), "Incorrect client ID value.")

	willTopic, willMessage, willQos, willRetain, will := msg.Will()

	require.Equal(t, QoS1, willQos, "Incorrect will QoS")
	require.Equal(t, "will", willTopic, "Incorrect will topic value.")
	require.Equal(t, "send me home", string(willMessage), "Incorrect will message value.")
	require.True(t, will, "Incorrect will flag")
	require.False(t, willRetain, "Incorrect will retain flag")

	username, password := msg.Credentials()
	require.Equal(t, "volantmq", string(username), "Incorrect username value.")
	require.Equal(t, "verysecret", string(password), "Incorrect password value.")

	_, _, err = Decode(ProtocolV50, buf)
	require.NoError(t, err)
}

func TestConnectMessageDecode2(t *testing.T) {
	// missing last byte 't'
	buf := []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e',
	}

	_, _, err := Decode(ProtocolV311, buf)
	require.EqualError(t, err, ErrInsufficientDataSize.Error())

	_, _, err = Decode(ProtocolV50, buf)
	require.EqualError(t, err, ErrInsufficientDataSize.Error())

	// missing last byte 't'
	buf = []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		5,    // Protocol level 4
		206,  // connect flags 11001110, will QoS = 01
		0,    // Keep Alive MSB (0)
		10,   // Keep Alive LSB (10)
		0xFF, // Wrong proprty size
		0,    // Client ID MSB (0)
		7,    // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	_, _, err = Decode(ProtocolV50, buf)
	require.Error(t, err)
}

func TestConnectMessageDecode3(t *testing.T) {
	// missing last byte 't'
	buf := []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,
		0, // Client ID MSB (0)
		7, // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	_, _, err := Decode(ProtocolV311, buf)
	require.EqualError(t, err, ErrInsufficientDataSize.Error())

	_, _, err = Decode(ProtocolV50, buf)
	require.EqualError(t, err, ErrInsufficientDataSize.Error())
}

func TestConnectMessageDecode4(t *testing.T) {
	// extra bytes
	buf := []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
		'e', 'x', 't', 'r', 'a',
	}

	_, n, err := Decode(ProtocolV311, buf)

	require.NoError(t, err)
	require.Equal(t, 62, n)

	// extra bytes
	buf = []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		5,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
		'e', 'x', 't', 'r', 'a',
	}

	_, _, err = Decode(ProtocolV311, buf)

	require.Error(t, err)

	// extra bytes
	buf = []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		5,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,
		0, // Client ID MSB (0)
		7, // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
		'e', 'x', 't', 'r', 'a',
	}

	_, n, err = Decode(ProtocolV311, buf)
	require.NoError(t, err)
	require.Equal(t, 63, n)
}

func TestConnectMessageDecode5(t *testing.T) {
	// missing client Id, clean session == 0
	buf := []byte{
		byte(CONNECT << 4),
		53,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		204, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		0,   // Client ID LSB (0)
		0,   // Will Topic MSB (0)
		4,   // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	_, _, err := Decode(ProtocolV311, buf)

	require.Error(t, err)
}

func TestConnectMessageEncode(t *testing.T) {
	buf := []byte{
		byte(CONNECT << 4),
		62,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		8,   // Client ID LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		8, // Username ID LSB (7)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	msg := newTestConnect(t, ProtocolV311)

	err := msg.SetWill("will", []byte("send me home"), QoS1, false)
	require.NoError(t, err)

	msg.SetClean(true)
	err = msg.SetClientID([]byte("volantmq"))
	require.NoError(t, err)

	msg.SetKeepAlive(10)

	err = msg.SetCredentials([]byte("volantmq"), []byte("verysecret"))
	require.NoError(t, err)

	dst := make([]byte, 100)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")
	require.Equal(t, buf, dst[:n], "Error decoding message.")
	require.Equal(t, ProtocolV311, msg.Version())

	// V5.0
	buf = []byte{
		byte(CONNECT << 4),
		63,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		5,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Property length
		0,   // Client ID MSB (0)
		8,   // Client ID LSB (8)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		8, // Username ID LSB (8)
		'v', 'o', 'l', 'a', 'n', 't', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	msg = newTestConnect(t, ProtocolV50)

	err = msg.SetWill("will", []byte("send me home"), QoS1, false)
	require.NoError(t, err)

	msg.SetClean(true)
	err = msg.SetClientID([]byte("volantmq"))
	require.NoError(t, err)

	msg.SetKeepAlive(10)

	err = msg.SetCredentials([]byte("volantmq"), []byte("verysecret"))
	require.NoError(t, err)

	dst = make([]byte, 100)
	n, err = msg.Encode(dst)

	require.NoError(t, err, "Error decoding message")
	require.Equal(t, ProtocolV50, msg.Version())
	require.Equal(t, len(buf), n, "Error decoding message")
	require.Equal(t, buf, dst[:n], "Error decoding message")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestConnectDecodeEncodeEquiv(t *testing.T) {
	buf := []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	m, n, err := Decode(ProtocolV50, buf)
	msg, ok := m.(*Connect)
	require.Equal(t, true, ok, "Invalid message type")

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n, "Error decoding message.")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n2, "Error decoding message.")
	require.Equal(t, buf, dst[:n2], "Error decoding message.")

	_, n3, err := Decode(ProtocolV50, dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(buf), n3, "Error decoding message.")
}
