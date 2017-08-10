package mem

import "github.com/troian/surgemq/persistence/types"

type dbStatus struct {
	done chan struct{}
}

type impl struct {
	status dbStatus

	r    retained
	s    sessions
	subs subscriptions
	sys  system
}

// New allocate new persistence provider of boltDB type
func New(config *persistenceTypes.MemConfig) (p persistenceTypes.Provider, err error) {
	pl := &impl{}

	pl.status.done = make(chan struct{})

	pl.r = retained{
		status: &pl.status,
	}

	pl.s = sessions{
		status:   &pl.status,
		sessions: make(map[string]*persistenceTypes.SessionState),
	}

	pl.subs = subscriptions{
		status: &pl.status,
		subs:   make(map[string][]byte),
	}

	pl.sys = system{
		status: &pl.status,
	}

	return pl, nil
}

func (p *impl) System() (persistenceTypes.System, error) {
	select {
	case <-p.status.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	return &p.sys, nil
}

// Sessions
func (p *impl) Sessions() (persistenceTypes.Sessions, error) {
	select {
	case <-p.status.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	return &p.s, nil
}

// Retained
func (p *impl) Retained() (persistenceTypes.Retained, error) {
	select {
	case <-p.status.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	return &p.r, nil
}

// Subscriptions
func (p *impl) Subscriptions() (persistenceTypes.Subscriptions, error) {
	select {
	case <-p.status.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	return &p.subs, nil
}

// Shutdown provider
func (p *impl) Shutdown() error {
	select {
	case <-p.status.done:
		return persistenceTypes.ErrNotOpen
	default:
		close(p.status.done)
	}

	return nil
}
