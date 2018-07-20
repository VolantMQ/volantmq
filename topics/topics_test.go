package topics

import (
	"testing"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/subscriber"
	"github.com/VolantMQ/volantmq/topics/types"
	"github.com/stretchr/testify/require"
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

	require.EqualError(t, topicsTypes.ErrInvalidArgs, err.Error())

	tConfig = topicsTypes.MemConfig{
		Name: "mem",
	}
	_, err = New(tConfig)

	require.EqualError(t, topicsTypes.ErrUnknownProvider, err.Error())

}

func TestTopicsOpenCloseProvider(t *testing.T) {
	for _, p := range testProviders {
		prov, err := New(p.config)
		require.NoError(t, err)

		err = prov.Stop()
		require.NoError(t, err)

		err = prov.Shutdown()
		require.NoError(t, err)
	}
}

func TestTopicsSubscribeInvalidQoS(t *testing.T) {
	for _, p := range testProviders {
		prov, err := New(p.config)
		require.NoError(t, err)

		ops := &vlsubscriber.SubscriptionParams{
			Ops: mqttp.SubscriptionOptions(mqttp.QosType(3)),
		}

		_, _, err = prov.Subscribe("test", nil, ops)
		require.Error(t, mqttp.ErrInvalidQoS, err.Error())

		err = prov.Stop()
		require.NoError(t, err)

		err = prov.Shutdown()
		require.NoError(t, err)
	}
}

//func TestTopicsSubscribeInvalidMessage(t *testing.T) {
//	for _, p := range testProviders {
//		prov, err := New(p.config)
//		require.NoError(t, err)
//
//		p := &topicsTypes.SubscriptionParams{
//			Ops: packet.SubscriptionOptions(packet.QosType(3)),
//		}
//
//		_, _, err = prov.Subscribe("test", nil, p)
//		require.Error(t, packet.ErrInvalidQoS, err.Error())
//
//		err = prov.Close()
//		require.NoError(t, err)
//	}
//}

//func TestTopicsSubscription(t *testing.T) {
//	for _, p := range testProviders {
//		prov, err := New(p.config)
//		require.NoError(t, err)
//
//		sub1 := &subscriber.Type{}
//		p := &topicsTypes.SubscriptionParams{
//			Ops: packet.SubscriptionOptions(mqttp.QoS2),
//		}
//
//		qos, _, err := prov.Subscribe("sports/tennis/+/stats", sub1, p)
//
//		require.NoError(t, err)
//		require.Equal(t, mqttp.QoS2, qos)
//
//		err = prov.UnSubscribe("sports/tennis", sub1)
//
//		require.Error(t, err)
//
//		//var subs types.Subscribers
//		//
//		//subs, err = prov.Subscribers("sports/tennis/anzel/stats", message.QoS2)
//		//
//		//require.NoError(t, err)
//		//require.Equal(t, 1, len(subs))
//		//
//		//subs, err = prov.Subscribers("sports/tennis/anzel/stats", message.QoS1)
//		//
//		//require.NoError(t, err)
//		//require.Equal(t, 1, len(subs))
//
//		err = prov.UnSubscribe("sports/tennis/+/stats", sub1)
//
//		require.NoError(t, err)
//	}
//}

//func TestTopicsRetained(t *testing.T) {
//	for _, p := range testProviders {
//		prov, err := New(p.config)
//		require.NoError(t, err)
//
//		msg1 := newPublishMessageLarge("sport/tennis/ricardo/stats", 1)
//		err = prov.Retain(msg1)
//		require.NoError(t, err)
//
//		msg2 := newPublishMessageLarge("sport/tennis/andre/stats", 1)
//		err = prov.Retain(msg2)
//		require.NoError(t, err)
//
//		msg3 := newPublishMessageLarge("sport/tennis/andre/bio", 1)
//		err = prov.Retain(msg3)
//		require.NoError(t, err)
//
//		var msglist []*message.Publish
//
//		// ---
//
//		msglist, err = prov.Retained(msg1.Topic())
//
//		require.NoError(t, err)
//		require.Equal(t, 1, len(msglist))
//
//		// ---
//
//		msglist, err = prov.Retained(msg2.Topic())
//
//		require.NoError(t, err)
//		require.Equal(t, 1, len(msglist))
//
//		// ---
//
//		msglist, err = prov.Retained(msg3.Topic())
//
//		require.NoError(t, err)
//		require.Equal(t, 1, len(msglist))
//
//		// ---
//
//		msglist, err = prov.Retained("sport/tennis/andre/+")
//
//		require.NoError(t, err)
//		require.Equal(t, 2, len(msglist))
//
//		// ---
//
//		msglist, err = prov.Retained("sport/tennis/andre/#")
//
//		require.NoError(t, err)
//		require.Equal(t, 2, len(msglist))
//
//		// ---
//
//		msglist, err = prov.Retained("sport/tennis/+/stats")
//
//		require.NoError(t, err)
//		require.Equal(t, 2, len(msglist))
//
//		// ---
//
//		msglist, err = prov.Retained("sport/tennis/#")
//
//		require.NoError(t, err)
//		require.Equal(t, 3, len(msglist))
//	}
//}

//func TestMultilevelWildcards(t *testing.T) {
//	for _, p := range testProviders {
//
//		prov, err := New(p.config)
//		require.NoError(t, err)
//
//		var messageNotifier sync.WaitGroup
//
//		onPublish := func(id string, msg *message.Publish) {
//			messageNotifier.Done()
//		}
//
//		sub := subscriber.New(subscriber.Config{
//			ID:          "testID",
//			Offline:     onPublish,
//			Topics:      prov,
//			Version:     message.ProtocolV311,
//			OfflineQoS0: false,
//		})
//
//		_, _, err = sub.Subscribe("#", message.SubscriptionOptions(message.QoS2))
//		require.NoError(t, err)
//
//		testMsg := newPublishMessageLarge("bla/bla", message.QoS2)
//
//		messageNotifier.Add(1)
//		prov.Publish(testMsg)
//		require.False(t, waitTimeout(&messageNotifier, 5*time.Second))
//
//		err = sub.UnSubscribe("#")
//		require.NoError(t, err)
//
//		_, _, err = sub.Subscribe("bla/#", message.SubscriptionOptions(message.QoS2))
//		require.NoError(t, err)
//
//		messageNotifier.Add(2)
//
//		testMsg = newPublishMessageLarge("bla/bla", message.QoS2)
//		prov.Publish(testMsg)
//		testMsg = newPublishMessageLarge("bla", message.QoS2)
//		prov.Publish(testMsg)
//		require.False(t, waitTimeout(&messageNotifier, 5*time.Second))
//
//		testMsg = newPublishMessageLarge("/bla", message.QoS2)
//		prov.Publish(testMsg)
//		messageNotifier.Add(1)
//		require.True(t, waitTimeout(&messageNotifier, 5*time.Second))
//	}
//}

//func newPublishMessageLarge(topic string, qos message.QosType) *message.Publish {
//	m, _ := message.NewMessage(message.ProtocolV311, message.PUBLISH)
//
//	msg := m.(*message.Publish)
//
//	msg.SetPayload(make([]byte, 1024))
//	msg.SetTopic(topic) // nolint: errcheck
//	msg.SetQoS(qos)     // nolint: errcheck
//
//	return msg
//}
//
//// waitTimeout waits for the waitgroup for the specified max timeout.
//// Returns true if waiting timed out.
//func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
//	c := make(chan struct{})
//	go func() {
//		defer close(c)
//		wg.Wait()
//	}()
//	select {
//	case <-c:
//		return false // completed normally
//	case <-time.After(timeout):
//		return true // timed out
//	}
//}
