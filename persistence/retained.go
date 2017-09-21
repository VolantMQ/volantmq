package persistence

type retained struct {
	status  *dbStatus
	packets []PersistedPacket
}

func (r *retained) Load() ([]PersistedPacket, error) {
	return r.packets, nil
}

func (r *retained) Store(data []PersistedPacket) error {
	r.packets = data

	return nil
}

func (r *retained) Wipe() error {
	r.packets = []PersistedPacket{}
	return nil
}
