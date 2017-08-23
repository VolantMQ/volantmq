package auth

// AccessType acl type
type AccessType int

// Status auth
type Status int

// Error auth provider errors
type Error int

// nolint: golint
const (
	AccessTypeRead  AccessType = 1
	AccessTypeWrite            = 2
)

// nolint: golint
const (
	StatusAllow Status = 0
	StatusDeny         = 1
)

// nolint: golint
const (
	ErrInvalidArgs Error = iota
	ErrUnknownProvider
	ErrAlreadyExists
	ErrNotFound
	ErrNotOpen
	ErrInternal
)

var errorsDesc = map[Error]string{
	ErrInvalidArgs:     "auth: invalid arguments",
	ErrUnknownProvider: "auth: unknown provider",
	ErrAlreadyExists:   "auth: already exists",
	ErrNotFound:        "auth: not found",
	ErrNotOpen:         "auth: not open",
	ErrInternal:        "auth: internal error",
}

var statusDesc = map[Status]string{
	StatusAllow: "auth status: access granted",
	StatusDeny:  "auth status: access denied",
}

// Provider interface
type Provider interface {
	// Password try authenticate with username and password
	Password(string, string) Status
	// ACL check access type for client id with username
	ACL(id string, username string, topic string, accessType AccessType) Status
}

// SessionPermissions check session permissions
type SessionPermissions interface {
	ACL(id string, username string, topic string, accessType AccessType) Status
}

// Type return string representation of the type
func (t AccessType) Type() string {
	switch t {
	case AccessTypeRead:
		return "read"
	case AccessTypeWrite:
		return "write"
	}

	return ""
}

func (e Error) Error() string {
	if s, ok := errorsDesc[e]; ok {
		return s
	}

	return "auth: unknown error"
}

func (e Status) Error() string {
	if s, ok := statusDesc[e]; ok {
		return s
	}

	return "auth status: unknown status"
}
