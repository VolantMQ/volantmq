package mem

import (
	"sync"

	"github.com/VolantMQ/volantmq/persistence/types"
)

type sessions struct {
	status        *dbStatus
	messages      sync.Map
	state         sync.Map
	subscriptions sync.Map
}

func (s *sessions) SubscriptionsIterate(load func([]byte, []byte) error) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.subscriptions.Range(func(k, v interface{}) bool {
		id := k.([]byte)
		val := v.([]byte)

		if err := load(id, val); err != nil {
			return false
		}
		return true
	})

	return nil
}

func (s *sessions) SubscriptionStore(id []byte, data []byte) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.subscriptions.Store(id, data)

	return nil
}

func (s *sessions) SubscriptionDelete(id []byte) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.subscriptions.Delete(id)

	return nil
}

func (s *sessions) SubscriptionsWipe() error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.subscriptions.Range(func(k, v interface{}) bool {
		s.subscriptions.Delete(k)
		return true
	})

	return nil
}

func (s *sessions) MessageStore(id []byte, data []byte) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	messages := &persistenceTypes.SessionMessages{}
	messages.OutMessages = append(messages.OutMessages, data)

	if state, ok := s.messages.LoadOrStore(id, messages); ok {
		msg := state.(*persistenceTypes.SessionMessages)
		msg.OutMessages = append(msg.OutMessages, data)
	}

	return nil
}

func (s *sessions) MessagesLoad(id []byte) (*persistenceTypes.SessionMessages, error) {
	select {
	case <-s.status.done:
		return nil, persistenceTypes.ErrNotOpen
	default:
	}

	if st, ok := s.messages.Load(id); ok {
		state := st.(*persistenceTypes.SessionMessages)
		return state, nil
	}

	return nil, persistenceTypes.ErrNotFound
}

func (s *sessions) MessagesStore(id []byte, state *persistenceTypes.SessionMessages) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	if st, ok := s.messages.LoadOrStore(id, state); ok {
		state := st.(*persistenceTypes.SessionMessages)
		state.OutMessages = append(state.OutMessages, state.OutMessages...)
		state.UnAckMessages = append(state.UnAckMessages, state.UnAckMessages...)
	}

	return nil
}

func (s *sessions) MessagesWipe(id []byte) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.messages.Delete(id)

	return nil
}

func (s *sessions) StateStore(id []byte, state *persistenceTypes.SessionState) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.state.Store(id, state)

	return nil
}

func (s *sessions) StatesIterate(load func([]byte, *persistenceTypes.SessionState) error) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.state.Range(func(k, v interface{}) bool {
		id := k.([]byte)
		state := v.(*persistenceTypes.SessionState)
		if err := load(id, state); err != nil {
			return false
		}

		return true
	})

	return nil
}

func (s *sessions) StateWipe(id []byte) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.state.Delete(id)

	return nil
}

func (s *sessions) StatesWipe() error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.state.Range(func(k, v interface{}) bool {
		s.state.Delete(k)
		return true
	})

	return nil
}

func (s *sessions) Delete(id []byte) error {
	select {
	case <-s.status.done:
		return persistenceTypes.ErrNotOpen
	default:
	}

	s.messages.Delete(id)
	s.state.Delete(id)
	s.subscriptions.Delete(id)

	return nil
}
