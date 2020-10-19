package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"

	"github.com/VolantMQ/vlapi/vlauth"
	"github.com/ghodss/yaml"
)

const (
	defaultAuthACL = `^.*$`
)

type authACLConfig struct {
	Read  string `yaml:"read" mapstructure:"read"`
	Write string `yaml:"write" mapstructure:"write"`
}

type authEnhUserConfig struct {
	User     string        `yaml:"user" mapstructure:"user"`
	Password string        `yaml:"password" mapstructure:"password"`
	ACL      authACLConfig `yaml:"acl" mapstructure:"acl"`
}

type authConfig struct {
	UsersFile  string              `yaml:"usersFile" mapstructure:"usersFile"`
	Users      map[string]string   `yaml:"users" mapstructure:"users"`
	EnhUsers   []authEnhUserConfig `yaml:"enhUsers" mapstructure:"enhUsers"`
	DefaultACL authACLConfig       `yaml:"defaultAcl" mapstructure:"defaultAcl"`
}

type authACL struct {
	read  *regexp.Regexp
	write *regexp.Regexp
}

type creds struct {
	hash string
	authACL
}

type simpleAuth struct {
	creds map[string]creds
	authACL
}

var _ vlauth.IFace = (*simpleAuth)(nil)

func newSimpleAuth(cfg authConfig) (*simpleAuth, error) {
	s := &simpleAuth{
		creds: make(map[string]creds),
	}

	if cfg.DefaultACL.Read == "" {
		logger.Warn("\tsimpleAuth config does not have defaultAcl.read. defaulting to \"^.*$\"")
		cfg.DefaultACL.Read = defaultAuthACL
	}

	if cfg.DefaultACL.Write == "" {
		logger.Warn("\tsimpleAuth config does not have defaultAcl.write. defaulting to \"^.*$\"")
		cfg.DefaultACL.Write = defaultAuthACL
	}

	var err error
	if s.read, err = regexp.Compile(cfg.DefaultACL.Read); err != nil {
		return nil, err
	}

	if s.write, err = regexp.Compile(cfg.DefaultACL.Write); err != nil {
		return nil, err
	}

	for u, p := range cfg.Users {
		s.creds[u] = creds{
			hash:    p,
			authACL: s.authACL,
		}
	}

	eLoad := func(users []authEnhUserConfig) error {
		for _, entry := range users {
			acl := authACL{}

			var e error

			if entry.ACL.Read == "" {
				acl.read = s.read
			} else if acl.read, e = regexp.Compile(entry.ACL.Read); e != nil {
				return e
			}

			if entry.ACL.Write == "" {
				acl.read = s.write
			} else if acl.write, e = regexp.Compile(entry.ACL.Write); e != nil {
				return e
			}

			s.creds[entry.User] = creds{
				hash:    entry.Password,
				authACL: acl,
			}
		}

		return nil
	}

	if err = eLoad(cfg.EnhUsers); err != nil {
		return nil, err
	}

	if cfg.UsersFile != "" {
		var f *os.File
		if f, err = os.Open(cfg.UsersFile); err != nil {
			return nil, fmt.Errorf("cannot open users file %w", err)
		}

		defer func() {
			_ = f.Close()
		}()

		var uData []byte
		if uData, err = ioutil.ReadAll(f); err != nil {
			return nil, fmt.Errorf("cannot read users file %w", err)
		}

		var uFile []authEnhUserConfig

		if err = yaml.Unmarshal(uData, &uFile); err != nil {
			return nil, fmt.Errorf("cannot unmarshal users file %w", err)
		}

		if err = eLoad(uFile); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (a *simpleAuth) addUser(u, p string, r, w string) error {
	c := creds{
		hash: p,
	}

	var err error

	if r != "" {
		if c.read, err = regexp.Compile(r); err != nil {
			return err
		}
	}

	if w != "" {
		if c.write, err = regexp.Compile(w); err != nil {
			return err
		}
	}

	a.creds[u] = c

	return nil
}

// Password ...
func (a *simpleAuth) Password(_, user, password string) error {
	if cred, ok := a.creds[user]; ok {
		algo := sha256.New()
		if _, err := algo.Write([]byte(password)); err == nil {
			if hex.EncodeToString(algo.Sum(nil)) == cred.hash {
				return vlauth.StatusAllow
			}
		}
	}
	return vlauth.StatusDeny
}

// ACL ...
func (a *simpleAuth) ACL(_, user, topic string, access vlauth.AccessType) error {
	cred, ok := a.creds[user]
	if !ok {
		return vlauth.StatusDeny
	}

	switch access {
	case vlauth.AccessRead:
		if cred.read.Match([]byte(topic)) {
			return vlauth.StatusAllow
		}
	case vlauth.AccessWrite:
		if cred.write.Match([]byte(topic)) {
			return vlauth.StatusAllow
		}
	}

	return vlauth.StatusDeny
}

// Shutdown ...
func (a *simpleAuth) Shutdown() error {
	a.creds = nil
	return nil
}
