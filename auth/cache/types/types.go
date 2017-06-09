package types

import (
	authTypes "github.com/troian/surgemq/auth/types"
)

// Provider
type Provider interface {
	Check(user, topic string, access authTypes.AccessType) error
	Update(user string, access authTypes.AccessType, topic string) error
	Invalidate(user string) error
	Shutdown() error
}

// ProviderConfig
type ProviderConfig interface{}

// BoltDBConfig
type BoltDBConfig struct {
	File string
}

var _ ProviderConfig = (*BoltDBConfig)(nil)
