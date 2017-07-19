package mem

import (
	"sync"

	"github.com/troian/surgemq/persistence/types"
)

type sessions struct {
	status *dbStatus

	lock sync.Mutex

	sessions map[string]*persistenceTypes.SessionState
}

func (s *sessions) PutOutMessage(id []byte, data []byte) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	if _, ok := s.sessions[string(id)]; !ok {
		s.sessions[string(id)] = &persistenceTypes.SessionState{}
	}

	s.sessions[string(id)].OutMessages = append(s.sessions[string(id)].OutMessages, data)

	return nil
}

// Get
func (s *sessions) Get(id []byte) (*persistenceTypes.SessionState, error) {
	select {
	case <-s.status.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	ses, ok := s.sessions[string(id)]

	if !ok {
		return nil, persistenceTypes.ErrNotFound
	}

	return ses, nil
}

// Load
func (s *sessions) Load(load func([]byte, *persistenceTypes.SessionState)) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	for id, state := range s.sessions {
		load([]byte(id), state)
	}

	return nil
}

// Store session state
func (s *sessions) Store(id []byte, state *persistenceTypes.SessionState) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	if st, ok := s.sessions[string(id)]; ok {
		st.OutMessages = append(st.OutMessages, state.OutMessages...)
		st.UnAckMessages = append(st.UnAckMessages, state.UnAckMessages...)
	} else {
		s.sessions[string(id)] = state
	}

	return nil
}

// Delete
func (s *sessions) Delete(id []byte) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	delete(s.sessions, string(id))

	return nil
}

// Wipe
func (s *sessions) Wipe() error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	defer s.lock.Unlock()
	s.lock.Lock()

	s.sessions = make(map[string]*persistenceTypes.SessionState)

	return nil
}
