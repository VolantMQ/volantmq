package persistence

import (
	"os"
	"testing"

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

			session, err = sessions.New("test1")
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

			err = pr.Shutdown()
			require.NoError(t, err)

			err = p.wrap.cleanup()
			require.NoError(t, err)
		})
	}
}

func TestMessagesAndRetained(t *testing.T) {
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

func TestMessagesAndSubscriptions(t *testing.T) {
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

func TestRetainedAndSubscriptions(t *testing.T) {
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

func TestComplete(t *testing.T) {
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
