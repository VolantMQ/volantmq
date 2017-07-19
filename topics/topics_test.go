package topics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/topics/types"
	"github.com/troian/surgemq/types"
)

type providerTest struct {
	name   string
	config topicsTypes.ProviderConfig
}

var testProviders []*providerTest

func init() {
	testProviders = append(testProviders, &providerTest{
		name:   "mem",
		config: topicsTypes.NewMemConfig(),
	})
}

func TestTopicsUnknownProvider(t *testing.T) {
	var tConfig topicsTypes.ProviderConfig

	_, err := New(tConfig)

	require.EqualError(t, types.ErrInvalidArgs, err.Error())

	tConfig = topicsTypes.MemConfig{
		Name: "mem",
	}
	_, err = New(tConfig)

	require.EqualError(t, types.ErrUnknownProvider, err.Error())

}

func TestTopicsOpenCloseProvider(t *testing.T) {
	for _, p := range testProviders {
		prov, err := New(p.config)
		require.NoError(t, err)

		err = prov.Close()
		require.NoError(t, err)
	}
}

func TestTopicsSubscribeInvalidQoS(t *testing.T) {
	for _, p := range testProviders {
		prov, err := New(p.config)
		require.NoError(t, err)

		_, err = prov.Subscribe("test", message.QosType(3), nil)
		require.Error(t, message.ErrInvalidQoS, err.Error())

		err = prov.Close()
		require.NoError(t, err)
	}
}

func TestTopicsSubscribeInvalidMessage(t *testing.T) {
	for _, p := range testProviders {
		prov, err := New(p.config)
		require.NoError(t, err)

		_, err = prov.Subscribe("test", message.QosType(3), nil)
		require.Error(t, message.ErrInvalidQoS, err.Error())

		err = prov.Close()
		require.NoError(t, err)
	}
}

func TestTopicsSubscription(t *testing.T) {
	for _, p := range testProviders {
		prov, err := New(p.config)
		require.NoError(t, err)

		sub1 := &types.Subscriber{}
		qos, err := prov.Subscribe("sports/tennis/+/stats", message.QoS2, sub1)

		require.NoError(t, err)
		require.Equal(t, message.QoS2, qos)

		err = prov.UnSubscribe("sports/tennis", sub1)

		require.Error(t, err)

		var subs types.Subscribers

		err = prov.Subscribers("sports/tennis/anzel/stats", message.QoS2, &subs)

		require.NoError(t, err)
		require.Equal(t, 1, len(subs))

		subs = types.Subscribers{}

		err = prov.Subscribers("sports/tennis/anzel/stats", message.QoS1, &subs)

		require.NoError(t, err)
		require.Equal(t, 1, len(subs))

		err = prov.UnSubscribe("sports/tennis/+/stats", sub1)

		require.NoError(t, err)
	}
}

func TestTopicsRetained(t *testing.T) {
	for _, p := range testProviders {
		prov, err := New(p.config)
		require.NoError(t, err)

		msg1 := newPublishMessageLarge("sport/tennis/ricardo/stats", 1)
		err = prov.Retain(msg1)
		require.NoError(t, err)

		msg2 := newPublishMessageLarge("sport/tennis/andre/stats", 1)
		err = prov.Retain(msg2)
		require.NoError(t, err)

		msg3 := newPublishMessageLarge("sport/tennis/andre/bio", 1)
		err = prov.Retain(msg3)
		require.NoError(t, err)

		var msglist []*message.PublishMessage

		// ---

		msglist, err = prov.Retained(msg1.Topic())

		require.NoError(t, err)
		require.Equal(t, 1, len(msglist))

		// ---

		msglist, err = prov.Retained(msg2.Topic())

		require.NoError(t, err)
		require.Equal(t, 1, len(msglist))

		// ---

		msglist, err = prov.Retained(msg3.Topic())

		require.NoError(t, err)
		require.Equal(t, 1, len(msglist))

		// ---

		msglist, err = prov.Retained("sport/tennis/andre/+")

		require.NoError(t, err)
		require.Equal(t, 2, len(msglist))

		// ---

		msglist, err = prov.Retained("sport/tennis/andre/#")

		require.NoError(t, err)
		require.Equal(t, 2, len(msglist))

		// ---

		msglist, err = prov.Retained("sport/tennis/+/stats")

		require.NoError(t, err)
		require.Equal(t, 2, len(msglist))

		// ---

		msglist, err = prov.Retained("sport/tennis/#")

		require.NoError(t, err)
		require.Equal(t, 3, len(msglist))
	}
}

func newPublishMessageLarge(topic string, qos message.QosType) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetPayload(make([]byte, 1024))
	msg.SetTopic(topic) // nolint: errcheck
	msg.SetQoS(qos)     // nolint: errcheck

	return msg
}
