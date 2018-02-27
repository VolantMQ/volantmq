package clients

import (
	"sync"
	"time"

	"github.com/VolantMQ/mqttp"
	"github.com/VolantMQ/volantmq/types"
)

type expiryEvent interface {
	sessionTimer(string, bool)
}

type expiryConfig struct {
	expiryEvent
	id        string
	createdAt time.Time
	messenger types.TopicMessenger
	will      *packet.Publish
	expireIn  *uint32
	willDelay uint32
}

type expiry struct {
	expiryConfig
	expiringSince time.Time
	timerLock     sync.Mutex
	timer         *time.Timer
}

func newExpiry(c expiryConfig) *expiry {
	return &expiry{
		expiryConfig: c,
	}
}

func (s *expiry) start() {
	var timerPeriod uint32

	// if meet will requirements point that
	if s.will != nil && s.willDelay > 0 {
		timerPeriod = s.willDelay
	} else {
		s.will = nil
	}

	if s.expireIn != nil {
		// if will delay is set before and value less than expiration
		// then timer should fire 2 times
		if (timerPeriod > 0) && (timerPeriod < *s.expireIn) {
			*s.expireIn = *s.expireIn - timerPeriod
		} else {
			timerPeriod = *s.expireIn
			*s.expireIn = 0
		}
	}

	s.expiringSince = time.Now()
	s.timer = time.NewTimer(time.Duration(timerPeriod) * time.Second)
}

func (s *expiry) cancel() {
	if !s.timer.Stop() {
		s.timerLock.Lock()
		s.timerLock.Unlock() // nolint: megacheck
	}
}

func (s *expiry) timerCallback() {
	defer s.timerLock.Unlock()
	s.timerLock.Lock()

	// 1. check for will message available
	if s.will != nil {
		// publish if exists and wipe state
		s.messenger.Publish(s.will) // nolint: errcheck
		s.will = nil
		s.willDelay = 0
	}

	if s.expireIn == nil {
		// 2.a session has processed delayed will and there is nothing to do
		// completely shutdown the session
		s.sessionTimer(s.id, false)
	} else if *s.expireIn == 0 {
		// session has expired. WIPE IT
		//if s.subscriber != nil {
		//	s.shutdownSubscriber(s.subscriber)
		//}
		s.sessionTimer(s.id, true)
	} else {
		// restart timer and wait again
		val := *s.expireIn
		// clear value pointed by expireIn so when next fire comes we signal session is expired
		*s.expireIn = 0
		s.timer.Reset(time.Duration(val) * time.Second)
	}
}
