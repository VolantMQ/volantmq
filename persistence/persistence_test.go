package persistence

import (
	"os"

	"testing"

	"github.com/VolantMQ/volantmq/persistence/types"
	"github.com/stretchr/testify/require"
)

type configWrap struct {
	config persistenceTypes.ProviderConfig
}

func (c *configWrap) cleanup() error {
	switch t := c.config.(type) {
	case *persistenceTypes.BoltDBConfig:
		return os.Remove(t.File)
	}

	return nil
}

type providerTest struct {
	name string
	wrap configWrap
}

type dummyProvider struct{}

var _ persistenceTypes.ProviderConfig = (*dummyProvider)(nil)

var testProviders []*providerTest

func init() {
	testProviders = append(testProviders, &providerTest{
		name: "boltdb",
		wrap: configWrap{
			config: &persistenceTypes.BoltDBConfig{
				File: "./persist.db",
			},
		},
	})

	testProviders = append(testProviders, &providerTest{
		name: "mem",
		wrap: configWrap{
			config: &persistenceTypes.MemConfig{},
		},
	})
}

func TestProvider(t *testing.T) {
	var config persistenceTypes.ProviderConfig

	//var pr types.Provider

	_, err := New(config)
	require.EqualError(t, err, persistenceTypes.ErrInvalidArgs.Error())

	config = &dummyProvider{}

	_, err = New(config)
	require.EqualError(t, err, persistenceTypes.ErrUnknownProvider.Error())
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
			require.EqualError(t, err, persistenceTypes.ErrNotOpen.Error())

			err = p.wrap.cleanup()
			require.NoError(t, err)
		})
	}
}
