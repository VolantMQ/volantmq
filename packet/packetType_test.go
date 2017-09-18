package packet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageTypeValid(t *testing.T) {
	tp := RESERVED
	ok, _ := tp.Valid(ProtocolV311)
	require.False(t, ok)

	tp = AUTH
	ok, _ = tp.Valid(ProtocolV311)
	require.False(t, ok)

	ok, _ = tp.Valid(ProtocolV50)
	require.True(t, ok)

	tp = Type(200)
	ok, _ = tp.Valid(ProtocolV50)
	require.False(t, ok)

	tp = PUBLISH
	ok, _ = tp.Valid(ProtocolV50)
	require.True(t, ok)
}

func TestMessageTypeNewMessage(t *testing.T) {
	tp := RESERVED

	msg, err := New(ProtocolV311, tp)
	require.EqualError(t, ErrInvalidMessageType, err.Error())
	require.Nil(t, msg)

	tp = AUTH
	msg, err = New(ProtocolV311, tp)
	require.EqualError(t, ErrInvalidMessageType, err.Error())
	require.Nil(t, msg)

	msg, err = New(ProtocolV50, tp)
	require.NoError(t, err)
	require.NotNil(t, msg)

	tp = Type(143)
	msg, err = New(ProtocolV50, tp)
	require.EqualError(t, ErrInvalidMessageType, err.Error())
	require.Nil(t, msg)

	tp = CONNACK
	msg, err = New(ProtocolV50, tp)
	require.NoError(t, err)
	require.Equal(t, CONNACK, msg.Type())
}

func TestMessageTypeDefaultFlags(t *testing.T) {
	require.Equal(t, uint8(0), RESERVED.DefaultFlags())
	require.Equal(t, uint8(0), CONNECT.DefaultFlags())
	require.Equal(t, uint8(0), CONNACK.DefaultFlags())
	require.Equal(t, uint8(0), PUBLISH.DefaultFlags())
	require.Equal(t, uint8(0), PUBACK.DefaultFlags())
	require.Equal(t, uint8(0), PUBREC.DefaultFlags())
	require.Equal(t, uint8(2), PUBREL.DefaultFlags())
	require.Equal(t, uint8(0), PUBCOMP.DefaultFlags())
	require.Equal(t, uint8(2), SUBSCRIBE.DefaultFlags())
	require.Equal(t, uint8(0), SUBACK.DefaultFlags())
	require.Equal(t, uint8(2), UNSUBSCRIBE.DefaultFlags())
	require.Equal(t, uint8(0), UNSUBACK.DefaultFlags())
	require.Equal(t, uint8(0), PINGREQ.DefaultFlags())
	require.Equal(t, uint8(0), PINGRESP.DefaultFlags())
	require.Equal(t, uint8(0), DISCONNECT.DefaultFlags())
	require.Equal(t, uint8(0), AUTH.DefaultFlags())

	tp := Type(200)
	require.Equal(t, uint8(0), tp.DefaultFlags())
}

func TestMessageTypeDesc(t *testing.T) {
	tp := Type(200)
	require.Equal(t, "UNKNOWN", tp.Desc())
}

func TestMessageTypeName(t *testing.T) {
	tp := Type(200)
	require.Equal(t, "UNKNOWN", tp.Name())
}
