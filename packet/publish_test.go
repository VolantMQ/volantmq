package packet

import (
	"testing"

	"time"

	"github.com/stretchr/testify/require"
)

func TestPublishDecode(t *testing.T) {
	raw := []byte{
		byte(PUBLISH << 4),
		7,    // remaining length
		0, 5, // topic length
		't', 'o', 'p', 'i', 'c',
	}

	_, _, err := Decode(ProtocolV311, raw)
	require.NoError(t, err)

	_, _, err = Decode(ProtocolV50, raw)
	// must fail because there is no property length
	require.EqualError(t, err, CodeMalformedPacket.Desc())
}

func TestPublishDecode1(t *testing.T) {
	raw := []byte{
		byte(PUBLISH << 4),
		8,    // remaining length
		0, 5, // topic length
		't', 'o', 'p', 'i', 'c',
		0,
	}

	_, _, err := Decode(ProtocolV311, raw)
	// should not fail as property length decoded as payload
	require.NoError(t, err)

	_, _, err = Decode(ProtocolV50, raw)
	require.NoError(t, err)
}

func TestPublishExpire(t *testing.T) {
	pkt := NewPublish(ProtocolV50)
	require.NotNil(t, pkt)

	_, _, expired := pkt.Expired()

	require.False(t, expired)
	pkt.SetExpireAt(time.Now().Add(2 * time.Second))
	time.Sleep(3 * time.Second)

	_, _, expired = pkt.Expired()
	require.True(t, expired)

	pkt.SetExpireAt(time.Now().Add(3 * time.Second))

	var expLeft uint32
	_, expLeft, expired = pkt.Expired()
	require.False(t, expired)

	err := pkt.PropertySet(PropertyPublicationExpiry, expLeft)
	require.NoError(t, err)

	prop := pkt.PropertyGet(PropertyPublicationExpiry)
	require.NotNil(t, prop)
	val, err := prop.AsInt()
	require.NoError(t, err)
	require.True(t, val <= 3 && val > 0)
}
