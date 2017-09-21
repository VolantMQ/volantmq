package clients

import (
	"sync"
	"time"

	"strconv"

	"github.com/VolantMQ/volantmq/connection"
	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/persistence"
	"github.com/VolantMQ/volantmq/subscriber"
	"github.com/VolantMQ/volantmq/types"
)

type exitReason int

const (
	exitReasonClean exitReason = iota
	exitReasonShutdown
	exitReasonExpired
)

type switchStatus int

const (
	swStatusSwitched switchStatus = iota
	swStatusIsOnline
	swStatusFinalized
)

type onSessionClose func(string, exitReason)
type onDisconnect func(string, packet.ReasonCode, bool)
type onSubscriberShutdown func(subscriber.ConnectionProvider)

type sessionEvents struct {
	signalClose        onSessionClose
	signalDisconnected onDisconnect
	shutdownSubscriber onSubscriberShutdown
}

type sessionPreConfig struct {
	sessionEvents
	id        string
	createdAt time.Time
	messenger types.TopicMessenger
}

type sessionReConfig struct {
	subscriber       subscriber.ConnectionProvider
	will             *packet.Publish
	expireIn         *uint32
	willDelay        uint32
	killOnDisconnect bool
}

type session struct {
	sessionEvents
	*sessionReConfig
	id             string
	idLock         *sync.Mutex
	messenger      types.TopicMessenger
	createdAt      time.Time
	expiringSince  time.Time
	lock           sync.Mutex
	connStop       *types.Once
	disconnectOnce *types.OnceWait
	wgDisconnected sync.WaitGroup
	conn           *connection.Type
	timer          *time.Timer
	timerLock      sync.Mutex
	finalized      bool
	isOnline       chan struct{}
}

type sessionWrap struct {
	s    *session
	lock sync.Mutex
}

func (s *sessionWrap) acquire() {
	s.lock.Lock()
}

func (s *sessionWrap) release() {
	s.lock.Unlock()
}

func (s *sessionWrap) swap(w *sessionWrap) *session {
	s.s = w.s
	s.s.idLock = &s.lock
	return s.s
}

func newSession(c *sessionPreConfig) *session {
	s := &session{
		sessionEvents: c.sessionEvents,
		id:            c.id,
		createdAt:     c.createdAt,
		messenger:     c.messenger,
		isOnline:      make(chan struct{}),
	}

	s.timer = time.AfterFunc(10*time.Second, s.timerCallback)
	s.timer.Stop()

	close(s.isOnline)
	return s
}

func (s *session) reconfigure(c *sessionReConfig, runExpiry bool) {
	s.sessionReConfig = c
	s.finalized = false
	if runExpiry {
		s.runExpiry(true)
	}
}

func (s *session) allocConnection(c *connection.PreConfig) error {
	cfg := &connection.Config{
		PreConfig:        c,
		ID:               s.id,
		OnDisconnect:     s.onDisconnect,
		Subscriber:       s.subscriber,
		Messenger:        s.messenger,
		KillOnDisconnect: s.killOnDisconnect,
		ExpireIn:         s.expireIn,
	}

	s.disconnectOnce = &types.OnceWait{}
	s.connStop = &types.Once{}

	var err error
	s.conn, err = connection.New(cfg)

	return err
}

func (s *session) start() {
	s.isOnline = make(chan struct{})
	s.wgDisconnected.Add(1)
	s.conn.Start()
	s.idLock.Unlock()
}

func (s *session) stop(reason packet.ReasonCode) *persistence.SessionState {
	s.connStop.Do(func() {
		if s.conn != nil {
			s.conn.Stop(reason)
			s.conn = nil
		}
	})

	s.wgDisconnected.Wait()

	if !s.timer.Stop() {
		s.timerLock.Lock()
		s.timerLock.Unlock() // nolint: megacheck
	}

	if !s.finalized {
		s.signalClose(s.id, exitReasonShutdown)
		s.finalized = true
	}

	state := &persistence.SessionState{
		Timestamp: s.createdAt.Format(time.RFC3339),
	}

	if s.expireIn != nil || (s.willDelay > 0 && s.will != nil) {
		state.Expire = &persistence.SessionDelays{
			Since: s.expiringSince.Format(time.RFC3339),
		}

		elapsed := uint32(time.Since(s.expiringSince) / time.Second)

		if (s.willDelay > 0 && s.will != nil) && (s.willDelay-elapsed) > 0 {
			s.willDelay = s.willDelay - elapsed
			s.will.SetPacketID(0)
			if buf, err := packet.Encode(s.will); err != nil {

			} else {
				state.Expire.WillIn = strconv.Itoa(int(s.willDelay))
				state.Expire.WillData = buf
			}
		}

		if s.expireIn != nil && *s.expireIn > 0 && (*s.expireIn-elapsed) > 0 {
			*s.expireIn = *s.expireIn - elapsed
		}
	}

	return state
}

// setOnline try switch session state from offline to online. This is necessary when
// when previous network connection has set session expiry or will delay or both
// if switch is successful then swStatusSwitched returned.
// if session has active network connection then returned value is swStatusIsOnline
// if connection has been closed and must not be used anymore then it returns swStatusFinalized
func (s *session) setOnline() switchStatus {
	isOnline := false
	// check session online status
	s.lock.Lock()
	select {
	case <-s.isOnline:
	default:
		isOnline = true
	}
	s.lock.Unlock()

	status := swStatusSwitched
	if !isOnline {
		// session is offline. before making any further step wait disconnect procedure is done
		s.wgDisconnected.Wait()

		// if stop returns false timer has been fired and there is goroutine might be running
		if !s.timer.Stop() {
			s.timerLock.Lock()
			s.timerLock.Unlock() // nolint: megacheck
		}

		if s.finalized {
			status = swStatusFinalized
		}
	} else {
		status = swStatusIsOnline
	}

	return status
}

func (s *session) runExpiry(will bool) {
	var timerPeriod uint32

	// if meet will requirements point that
	if will && s.will != nil && s.willDelay > 0 {
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
	s.timer.Reset(time.Duration(timerPeriod) * time.Second)
}

func (s *session) onDisconnect(p *connection.DisconnectParams) {
	s.disconnectOnce.Do(func() {
		defer s.wgDisconnected.Done()

		s.lock.Lock()
		close(s.isOnline)
		s.lock.Unlock()

		finalize := func(err exitReason) {
			s.signalClose(s.id, err)
			s.finalized = true
		}

		s.connStop.Do(func() {
			s.conn = nil
		})

		if p.ExpireAt != nil {
			s.expireIn = p.ExpireAt
		}

		// If session expiry is set to 0, the Session ends when the Network Connection is closed
		if s.expireIn != nil && *s.expireIn == 0 {
			s.killOnDisconnect = true
		}

		// valid willMsg pointer tells we have will message
		// if session is clean send will regardless to will delay
		if p.Will && s.will != nil && (s.killOnDisconnect || s.willDelay == 0) {
			s.messenger.Publish(s.will) // nolint: errcheck
			s.will = nil
		}

		s.signalDisconnected(s.id, p.Reason, !s.killOnDisconnect)

		if s.killOnDisconnect || !s.subscriber.HasSubscriptions() {
			s.shutdownSubscriber(s.subscriber)
			s.subscriber = nil
		}

		if s.killOnDisconnect {
			defer finalize(exitReasonClean)
		} else {
			// check if remaining subscriptions exists, expiry is presented and will delay not set to 0
			if s.expireIn == nil && s.willDelay == 0 {
				// signal to shutdown session
				defer finalize(exitReasonShutdown)
			} else if (s.expireIn != nil && *s.expireIn > 0) || s.willDelay > 0 {
				// new expiry value might be received upon disconnect message from the client
				if p.ExpireAt != nil {
					s.expireIn = p.ExpireAt
				}

				s.runExpiry(p.Will)
			}
		}
	})
}

func (s *session) timerCallback() {
	defer s.timerLock.Unlock()
	s.timerLock.Lock()

	finalize := func(reason exitReason) {
		s.signalClose(s.id, reason)
		s.finalized = true
	}

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
		defer finalize(exitReasonShutdown)
	} else if *s.expireIn == 0 {
		// session has expired. WIPE IT
		if s.subscriber != nil {
			s.shutdownSubscriber(s.subscriber)
		}
		defer finalize(exitReasonExpired)
	} else {
		// restart timer and wait again
		val := *s.expireIn
		// clear value pointed by expireIn so when next fire comes we signal session is expired
		*s.expireIn = 0
		s.timer.Reset(time.Duration(val) * time.Second)
	}
}
