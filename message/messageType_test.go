package message

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageTypeValid(t *testing.T) {
	tp := RESERVED
	require.Equal(t, false, tp.Valid())

	tp = RESERVED2
	require.Equal(t, false, tp.Valid())

	tp = Type(200)
	require.Equal(t, false, tp.Valid())

	tp = PUBLISH
	require.Equal(t, true, tp.Valid())
}

func TestMessageTypeNewMessage(t *testing.T) {
	tp := RESERVED

	msg, err := tp.NewMessage()
	require.EqualError(t, ErrInvalidMessageType, err.Error())
	require.Equal(t, nil, msg)

	msg, err = NewMessage(tp)
	require.EqualError(t, ErrInvalidMessageType, err.Error())
	require.Equal(t, nil, msg)

	tp = RESERVED2
	msg, err = tp.NewMessage()
	require.EqualError(t, ErrInvalidMessageType, err.Error())
	require.Equal(t, nil, msg)

	msg, err = NewMessage(tp)
	require.EqualError(t, ErrInvalidMessageType, err.Error())
	require.Equal(t, nil, msg)

	tp = Type(143)

	msg, err = tp.NewMessage()
	require.EqualError(t, ErrInvalidMessageType, err.Error())
	require.Equal(t, nil, msg)

	msg, err = NewMessage(tp)
	require.EqualError(t, ErrInvalidMessageType, err.Error())
	require.Equal(t, nil, msg)

	tp = CONNACK

	msg, err = tp.NewMessage()
	require.NoError(t, err)
	require.Equal(t, CONNACK, msg.Type())

	msg, err = NewMessage(tp)
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
	require.Equal(t, uint8(0), RESERVED2.DefaultFlags())

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
