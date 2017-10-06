package connection

import (
	"errors"
)

func (s *impl) runKeepAlive() {
	if s.keepAlive > 0 {
		s.keepAliveTimer.Reset(s.keepAlive)
	}
}

func (s *impl) keepAliveFired() {
	s.onConnectionClose(errors.New("time out"))
}
