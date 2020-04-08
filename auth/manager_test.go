package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/VolantMQ/vlapi/vlauth"
	"github.com/stretchr/testify/require"
)

type testAuth struct {
	creds map[string]string
}

var _ vlauth.IFace = (*testAuth)(nil)

func newSimpleAuth() *testAuth {
	return &testAuth{
		creds: map[string]string{
			"testuser": "9f735e0df9a1ddc702bf0a1a7b83033f9f7153a00c29de82cedadc9957289b05",
		},
	}
}

func (a *testAuth) Password(_, user, password string) error {
	if hash, ok := a.creds[user]; ok {
		algo := sha256.New()
		if _, err := algo.Write([]byte(password)); err == nil {
			if hex.EncodeToString(algo.Sum(nil)) == hash {
				return vlauth.StatusAllow
			}
		}
	}
	return vlauth.StatusDeny
}

func (a *testAuth) ACL(_, _, _ string, acl vlauth.AccessType) error {
	if acl == vlauth.AccessWrite {
		return vlauth.StatusDeny
	}

	return vlauth.StatusAllow
}

func (a *testAuth) Shutdown() error {
	a.creds = nil
	return nil
}

func TestAuthRegister(t *testing.T) {
	err := Register("basic", newSimpleAuth())
	require.NoError(t, err)
}

func TestAuthRegisterDupe(t *testing.T) {
	err := Register("basic", newSimpleAuth())
	require.Error(t, err, "already exists")
}

func TestAuthRegisterInvalidArgs(t *testing.T) {
	err := Register("", newSimpleAuth())
	require.Error(t, err, "invalid args")

	err = Register("basic", nil)
	require.Error(t, err, "invalid args")
}

func TestNewManagerUnknownProvider(t *testing.T) {
	p, err := NewManager([]string{"bla"})
	require.Error(t, err)
	require.Nil(t, p)
}

func TestNewManagerKnownProvider(t *testing.T) {
	p, err := NewManager([]string{"basic"})
	require.NoError(t, err)
	require.NotNil(t, p)

	acl, st := p.Password("", "testuser", "testpassword")
	require.EqualError(t, st, vlauth.StatusAllow.Error())
	require.NotNil(t, acl)

	st = acl.ACL("", "testuser", "/topic", vlauth.AccessRead)
	require.EqualError(t, st, vlauth.StatusAllow.Error())

	st = acl.ACL("", "testuser", "/topic", vlauth.AccessWrite)
	require.EqualError(t, st, vlauth.StatusDeny.Error())

	acl, st = p.Password("", "", "")
	require.EqualError(t, st, vlauth.StatusDeny.Error())
	require.Nil(t, acl)

	acl, st = p.Password("", "testuser", "testpassword1")
	require.EqualError(t, st, vlauth.StatusDeny.Error())
	require.Nil(t, acl)
}

func TestAuthUnregister(t *testing.T) {
	UnRegister("basic")

	p, err := NewManager([]string{"basic"})
	require.Error(t, err)
	require.Nil(t, p)
}
