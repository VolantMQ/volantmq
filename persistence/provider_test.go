package persistence

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
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

			//var ses types.Session

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

			err = p.wrap.cleanup()
			require.NoError(t, err)
		})
	}
}
