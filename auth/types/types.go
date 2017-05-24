package types

// AccessType acl type
type AccessType int

const (
	// AuthAccessTypeRead read access
	AuthAccessTypeRead AccessType = 1
	// AuthAccessTypeWrite write access
	AuthAccessTypeWrite = 2
)
