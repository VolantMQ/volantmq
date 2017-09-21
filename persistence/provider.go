package persistence

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

// Default allocate new persistence provider of in memory type
func Default() Provider {
	pl := &impl{}

	pl.status.done = make(chan struct{})

	pl.r = retained{
		status: &pl.status,
	}

	pl.s = sessions{
		status: &pl.status,
		//sessions: make(map[string]*sessionState),
	}

	pl.subs = subscriptions{
		status: &pl.status,
		subs:   make(map[string][]byte),
	}

	pl.sys = system{
		status: &pl.status,
	}

	return pl
}

func (p *impl) System() (System, error) {
	select {
	case <-p.status.done:
		return nil, ErrNotOpen
	default:
	}

	return &p.sys, nil
}

// Sessions
func (p *impl) Sessions() (Sessions, error) {
	select {
	case <-p.status.done:
		return nil, ErrNotOpen
	default:
	}

	return &p.s, nil
}

// Retained
func (p *impl) Retained() (Retained, error) {
	select {
	case <-p.status.done:
		return nil, ErrNotOpen
	default:
	}

	return &p.r, nil
}

// Shutdown provider
func (p *impl) Shutdown() error {
	select {
	case <-p.status.done:
		return ErrNotOpen
	default:
		close(p.status.done)
	}

	return nil
}
