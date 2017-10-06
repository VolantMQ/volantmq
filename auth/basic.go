package auth

type Simple interface {
	Password(u, p string) Status
}

type Anonymous interface {
	Is() Status
}
