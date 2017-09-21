package persistence

import (
	"sync"
)

type sessions struct {
	status  *dbStatus
	entries sync.Map
}

type session struct {
	lock    sync.Mutex
	state   *SessionState
	packets []PersistedPacket
}

func (s *sessions) Exists(id []byte) bool {
	_, ok := s.entries.Load(string(id))
	return ok
}

func (s *sessions) SubscriptionsStore(id []byte, data []byte) error {
	elem, _ := s.entries.LoadOrStore(string(id), &session{})

	ses := elem.(*session)
	ses.lock.Lock()
	defer ses.lock.Unlock()
	if ses.state == nil {
		ses.state = &SessionState{}
	}

	ses.state.Subscriptions = data

	return nil
}

func (s *sessions) SubscriptionsDelete(id []byte) error {
	if elem, ok := s.entries.Load(string(id)); ok {
		ses := elem.(*session)
		ses.lock.Lock()
		defer ses.lock.Unlock()
		ses.state.Subscriptions = []byte{}
	}

	return nil
}

func (s *sessions) PacketsForEach(id []byte, load func(PersistedPacket) error) error {
	if elem, ok := s.entries.Load(string(id)); ok {
		ses := elem.(*session)

		for _, p := range ses.packets {
			load(p) // nolint: errcheck
		}
	}

	return nil
}

func (s *sessions) PacketsStore(id []byte, packets []PersistedPacket) error {
	if elem, ok := s.entries.Load(string(id)); ok {
		ses := elem.(*session)

		ses.lock.Lock()
		ses.packets = append(ses.packets, packets...)
		ses.lock.Unlock()
	}

	return nil
}

func (s *sessions) PacketsDelete(id []byte) error {
	if elem, ok := s.entries.Load(string(id)); ok {
		ses := elem.(*session)

		ses.lock.Lock()
		ses.packets = []PersistedPacket{}
		ses.lock.Unlock()
	}

	return nil
}

func (s *sessions) PacketStore(id []byte, packet PersistedPacket) error {
	if elem, ok := s.entries.Load(string(id)); ok {
		ses := elem.(*session)

		ses.lock.Lock()
		ses.packets = append(ses.packets, packet)
		ses.lock.Unlock()
	}
	return nil
}

func (s *sessions) LoadForEach(load func([]byte, *SessionState) error) error {
	var err error
	s.entries.Range(func(key, value interface{}) bool {
		sID := []byte(key.(string))
		ses := value.(*SessionState)
		err = load(sID, ses)
		return err == nil
	})

	return err
}

func (s *sessions) StateStore(id []byte, state *SessionState) error {
	elem, _ := s.entries.LoadOrStore(string(id), &session{})
	ses := elem.(*session)
	ses.lock.Lock()
	defer ses.lock.Unlock()

	if ses.state != nil {
		if len(ses.state.Subscriptions) > 0 && len(state.Subscriptions) == 0 {
			state.Subscriptions = ses.state.Subscriptions
		}
	}

	ses.state = state
	return nil
}

func (s *sessions) StateDelete(id []byte) error {
	if elem, ok := s.entries.Load(string(id)); ok {
		ses := elem.(*session)
		ses.lock.Lock()
		defer ses.lock.Unlock()
		ses.state = nil
	}

	return nil
}

// Delete
func (s *sessions) Delete(id []byte) error {
	s.entries.Delete(string(id))
	return nil
}
