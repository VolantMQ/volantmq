package persistence

type system struct {
	status *dbStatus
}

func (s *system) GetInfo() (*SystemState, error) {
	state := &SystemState{}

	return state, nil
}

func (s *system) SetInfo(state *SystemState) error {
	return nil
}
