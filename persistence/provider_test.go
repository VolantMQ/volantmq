package persistence

import (
	"os"
	"testing"

	"strconv"

	"github.com/stretchr/testify/require"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence/types"
)

type configWrap struct {
	config types.ProviderConfig
}

func (c *configWrap) cleanup() error {
	switch t := c.config.(type) {
	case *types.BoltDBConfig:
		return os.Remove(t.File)
	}

	return nil
}

type providerTest struct {
	name string
	wrap configWrap
}

type dummyProvider struct{}

var _ types.ProviderConfig = (*dummyProvider)(nil)

var testProviders []*providerTest

func init() {
	testProviders = append(testProviders, &providerTest{
		name: "boltdb",
		wrap: configWrap{
			config: &types.BoltDBConfig{
				File: "./persist.db",
			},
		},
	})
}

func TestProvider(t *testing.T) {
	var config types.ProviderConfig

	//var pr types.Provider

	_, err := New(config)
	require.EqualError(t, err, types.ErrInvalidArgs.Error())

	config = &dummyProvider{}

	_, err = New(config)
	require.EqualError(t, err, types.ErrUnknownProvider.Error())
}

func TestOpenClose(t *testing.T) {
	for _, p := range testProviders {
		t.Run(p.name, func(t *testing.T) {
			pr, err := New(p.wrap.config)
			require.NoError(t, err)

			err = pr.Shutdown()
			require.NoError(t, err)

			err = p.wrap.cleanup()
			require.NoError(t, err)
		})
	}
}

func TestReopen(t *testing.T) {
	for _, p := range testProviders {
		t.Run(p.name, func(t *testing.T) {
			pr, err := New(p.wrap.config)
			require.NoError(t, err)

			err = pr.Shutdown()
			require.NoError(t, err)

			pr, err = New(p.wrap.config)
			require.NoError(t, err)

			err = pr.Shutdown()
			require.NoError(t, err)

			err = pr.Shutdown()
			require.EqualError(t, err, types.ErrNotOpen.Error())

			err = p.wrap.cleanup()
			require.NoError(t, err)
		})
	}
}

func TestSessions(t *testing.T) {
	for _, p := range testProviders {
		t.Run(p.name, func(t *testing.T) {
			pr, err := New(p.wrap.config)
			require.NoError(t, err)

			var sessions types.Sessions

			sessions, err = pr.Sessions()
			require.NoError(t, err)

			err = sessions.Delete("unknown sessions")
			require.EqualError(t, err, types.ErrNotFound.Error())

			_, err = sessions.Get("unknown sessions")
			require.EqualError(t, err, types.ErrNotFound.Error())

			_, err = sessions.New("test1")
			require.NoError(t, err)

			err = pr.Shutdown()
			require.NoError(t, err)

			pr, err = New(p.wrap.config)
			require.NoError(t, err)

			sessions, err = pr.Sessions()
			require.NoError(t, err)

			_, err = sessions.Get("test1")
			require.NoError(t, err)

			_, err = sessions.New("test1")
			require.EqualError(t, err, types.ErrAlreadyExists.Error())

			err = sessions.Delete("test1")
			require.NoError(t, err)

			_, err = sessions.Get("test1")
			require.EqualError(t, err, types.ErrNotFound.Error())

			err = p.wrap.cleanup()
			require.NoError(t, err)
		})
	}
}

func TestSubscriptions(t *testing.T) {
	for _, p := range testProviders {
		t.Run(p.name, func(t *testing.T) {
			pr, err := New(p.wrap.config)
			require.NoError(t, err)

			var sessions types.Sessions

			sessions, err = pr.Sessions()
			require.NoError(t, err)

			var session types.Session
			session, err = sessions.New("test1")
			require.NoError(t, err)

			var subscriptions types.Subscriptions

			subscriptions, err = session.Subscriptions()
			require.NoError(t, err)

			_, err = subscriptions.Get()
			require.EqualError(t, err, types.ErrNotFound.Error())

			subsList := make(message.TopicsQoS)
			subsList["topic1"] = message.QosAtLeastOnce
			subsList["topic2"] = message.QosAtLeastOnce
			subsList["topic3"] = message.QosExactlyOnce

			err = subscriptions.Add(subsList)
			require.NoError(t, err)

			var subsList1 message.TopicsQoS
			subsList1, err = subscriptions.Get()
			require.NoError(t, err)
			require.Equal(t, len(subsList), len(subsList1))

			for topic, q := range subsList {
				require.Equal(t, q, subsList1[topic])
			}

			err = pr.Shutdown()
			require.NoError(t, err)

			pr, err = New(p.wrap.config)
			require.NoError(t, err)

			sessions, err = pr.Sessions()
			require.NoError(t, err)

			_, err = sessions.New("test1")
			require.EqualError(t, err, types.ErrAlreadyExists.Error())

			session, err = sessions.Get("test1")
			require.NoError(t, err)

			subscriptions, err = session.Subscriptions()
			require.NoError(t, err)

			subsList1, err = subscriptions.Get()
			require.NoError(t, err)

			for topic, q := range subsList {
				require.Equal(t, q, subsList1[topic])
			}

			err = pr.Shutdown()
			require.NoError(t, err)

			err = p.wrap.cleanup()
			require.NoError(t, err)
		})
	}
}

func TestRetained(t *testing.T) {
	for _, p := range testProviders {
		t.Run(p.name, func(t *testing.T) {
			pr, err := New(p.wrap.config)
			require.NoError(t, err)

			retained, err := pr.Retained()
			require.NoError(t, err)

			_, err = retained.Load()
			require.EqualError(t, err, types.ErrNotFound.Error())

			var messages []message.Provider
			for i := 1; i != 100; i++ {
				msg := message.NewPublishMessage()
				msg.SetPacketID(uint16(i))
				msg.SetRetain(true)
				msg.SetTopic("Topic:" + strconv.Itoa(i)) // nolint: errcheck
				if (i % 10) != 0 {
					msg.SetQoS(message.QosExactlyOnce) // nolint: errcheck
				} else {
					msg.SetQoS(message.QosAtLeastOnce) // nolint: errcheck
				}
			}

			err = retained.Store(messages)
			require.NoError(t, err)

			var messages1 []message.Provider
			retained, err = pr.Retained()
			require.NoError(t, err)

			messages1, err = retained.Load()
			require.NoError(t, err)
			require.Equal(t, len(messages), len(messages1))

			err = pr.Shutdown()
			require.NoError(t, err)

			pr, err = New(p.wrap.config)
			require.NoError(t, err)

			retained, err = pr.Retained()
			require.NoError(t, err)

			messages1, err = retained.Load()
			require.NoError(t, err)
			require.Equal(t, len(messages), len(messages1))

			err = retained.Delete()
			require.NoError(t, err)

			_, err = retained.Load()
			require.EqualError(t, err, types.ErrNotFound.Error())

			err = retained.Delete()
			require.EqualError(t, err, types.ErrNotFound.Error())

			err = pr.Shutdown()
			require.NoError(t, err)

			err = p.wrap.cleanup()
			require.NoError(t, err)
		})
	}
}

func TestMessages(t *testing.T) {
	for _, p := range testProviders {
		t.Run(p.name, func(t *testing.T) {
			pr, err := New(p.wrap.config)
			require.NoError(t, err)

			var sessions types.Sessions

			sessions, err = pr.Sessions()
			require.NoError(t, err)

			var session types.Session

			session, err = sessions.New("test1")
			require.NoError(t, err)

			var messages types.Messages
			messages, err = session.Messages()
			require.NoError(t, err)

			_, err = messages.Load()
			require.EqualError(t, err, types.ErrNotFound.Error())

			var rawMessages []message.Provider

			for i := 0; i < 100; i++ {
				m := message.NewPublishMessage()

				if (i % 10) != 0 {
					m.SetPacketID(uint16(i))
				}

				if (i % 2) != 0 {
					m.SetQoS(message.QosAtLeastOnce) // nolint: errcheck
				} else {
					m.SetQoS(message.QosExactlyOnce) // nolint: errcheck
				}

				m.SetTopic("test topic: " + strconv.Itoa(i)) // nolint: errcheck

				m.SetPayload([]byte("test payload: " + strconv.Itoa(i)))

				rawMessages = append(rawMessages, m)
			}

			err = messages.Store("out", rawMessages)
			require.NoError(t, err)

			var rawMessages1 *types.SessionMessages
			rawMessages1, err = messages.Load()
			require.NoError(t, err)

			require.Equal(t, len(rawMessages), len(rawMessages1.Out.Messages))
			require.Equal(t, 0, len(rawMessages1.In.Messages))

			verifyMessages := func() {
				for i, m := range rawMessages {
					switch mT := m.(type) {
					case *message.PublishMessage:
						mT1 := rawMessages1.Out.Messages[i].(*message.PublishMessage)
						require.Equal(t, mT.PacketID(), mT1.PacketID())
						require.Equal(t, mT.QoS(), mT1.QoS())
						require.Equal(t, mT.Topic(), mT1.Topic())
						require.Equal(t, mT.Payload(), mT1.Payload())
					default:
						require.Fail(t, "Expected message type *message.PublishMessage. Received %v", mT)
					}
				}
			}

			verifyMessages()

			err = pr.Shutdown()
			require.NoError(t, err)

			pr, err = New(p.wrap.config)
			require.NoError(t, err)

			sessions, err = pr.Sessions()
			require.NoError(t, err)

			_, err = sessions.New("test1")
			require.EqualError(t, err, types.ErrAlreadyExists.Error())

			session, err = sessions.Get("test1")
			require.NoError(t, err)

			messages, err = session.Messages()
			require.NoError(t, err)

			rawMessages1, err = messages.Load()
			require.NoError(t, err)

			require.Equal(t, len(rawMessages), len(rawMessages1.Out.Messages))
			require.Equal(t, 0, len(rawMessages1.In.Messages))

			verifyMessages()

			err = pr.Shutdown()
			require.NoError(t, err)

			err = p.wrap.cleanup()
			require.NoError(t, err)
		})
	}
}
