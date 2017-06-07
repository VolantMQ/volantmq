package types

import (
	"errors"
)

// AccessType acl type
type AccessType int

const (
	// AuthAccessTypeRead read access
	AuthAccessTypeRead AccessType = 1
	// AuthAccessTypeWrite write access
	AuthAccessTypeWrite = 2
)

var (
	// ErrDenied access denied
	ErrDenied = errors.New("access denied")

	// ErrInvalidArgs invalid arguments provided
	ErrInvalidArgs = errors.New("invalid arguments")

	// ErrUnknownProvider if provider is unknown
	ErrUnknownProvider = errors.New("Unknown provider")

	// ErrAlreadyExists object already exists
	ErrAlreadyExists = errors.New("Already exists")

	// ErrNotFound object not found
	ErrNotFound = errors.New("Not found")

	// ErrNotOpen storage is not open
	ErrNotOpen = errors.New("not open")

	// ErrInternal something bad happened
	ErrInternal = errors.New("internal error")
)

func (t AccessType) Type() string {
	switch t {
	case AuthAccessTypeRead:
		return "read"
	case AuthAccessTypeWrite:
		return "write"
	}

	return ""
}
