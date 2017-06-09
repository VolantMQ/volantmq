package cache

import (
	cacheType "github.com/troian/surgemq/auth/cache/types"
	authTypes "github.com/troian/surgemq/auth/types"

	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

type configWrap struct {
	config cacheType.ProviderConfig
}

func (c *configWrap) cleanup() error {
	switch t := c.config.(type) {
	case *cacheType.BoltDBConfig:
		return os.Remove(t.File)
	}

	return nil
}

type providerTest struct {
	name string
	wrap configWrap
}

type dummyProvider struct{}

var _ cacheType.ProviderConfig = (*dummyProvider)(nil)

var testProviders []*providerTest

func init() {
	testProviders = append(testProviders, &providerTest{
		name: "boltdb",
		wrap: configWrap{
			config: &cacheType.BoltDBConfig{
				File: "./persist.db",
			},
		},
	})
}

func TestProvider(t *testing.T) {
	var config cacheType.ProviderConfig

	_, err := New(config)
	require.EqualError(t, err, authTypes.ErrInvalidArgs.Error())

	config = &dummyProvider{}

	_, err = New(config)
	require.EqualError(t, err, authTypes.ErrUnknownProvider.Error())
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
			require.EqualError(t, err, authTypes.ErrNotOpen.Error())

			err = p.wrap.cleanup()
			require.NoError(t, err)
		})
	}
}
