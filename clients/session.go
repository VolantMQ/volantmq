package clients

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"fmt"

	"unsafe"

	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/connection"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/subscriber"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/types"
)

type exitReason int

const (
	exitReasonClean exitReason = iota
	exitReasonKeepSubscriber
	exitReasonShutdown
	exitReasonExpired
)

type onSessionClose func(string, exitReason)
type onSessionPersist func(string, *persistenceTypes.SessionMessages)
type onDisconnect func(string, bool, message.ReasonCode)

type sessionConfig struct {
	id           string
	createdAt    time.Time
	onPersist    onSessionPersist
	onClose      onSessionClose
	onDisconnect onDisconnect
	messenger    types.TopicMessenger
	clean        bool
}

type connectionConfig struct {
	username  string
	state     *persistenceTypes.SessionMessages
	metric    systree.Metric
	conn      net.Conn
	auth      auth.SessionPermissions
	keepAlive uint16
	sendQuota uint16
	version   message.ProtocolVersion
}

type setupConfig struct {
	subscriber subscriber.ConnectionProvider
	will       *message.PublishMessage
	expireIn   *time.Duration
	willDelay  time.Duration
}

type session struct {
	self             uintptr
	createdAt        time.Time
	id               string
	messenger        types.TopicMessenger
	subscriber       subscriber.ConnectionProvider
	notifyDisconnect onDisconnect
	onPersist        onSessionPersist
	onClose          onSessionClose
	startLock        sync.Mutex
	lock             sync.Mutex
	timerWorker      sync.WaitGroup
	wgStopped        sync.WaitGroup
	timerChan        chan struct{}
	connStop         *types.Once
	timer            *time.Timer
	conn             *connection.Type
	will             *message.PublishMessage
	expireIn         *time.Duration
	willDelay        time.Duration
	timerStartedAt   time.Time
	isOnline         uintptr
	clean            bool
}

func newSession(c *sessionConfig) (*session, error) {
	s := &session{
		id:               c.id,
		createdAt:        c.createdAt,
		clean:            c.clean,
		messenger:        c.messenger,
		onPersist:        c.onPersist,
		onClose:          c.onClose,
		notifyDisconnect: c.onDisconnect,
		connStop:         &types.Once{},
		isOnline:         1,
		timerChan:        make(chan struct{}),
	}

	s.self = uintptr(unsafe.Pointer(s))
	close(s.timerChan)

	return s, nil
}

func (s *session) acquire() {
	s.startLock.Lock()
}

func (s *session) release() {
	s.startLock.Unlock()
}

func (s *session) configure(c *setupConfig, runExpiry bool) {
	defer s.lock.Unlock()
	s.lock.Lock()

	s.will = c.will
	s.expireIn = c.expireIn
	s.willDelay = c.willDelay
	s.subscriber = c.subscriber
	s.isOnline = 1

	if runExpiry {
		s.runExpiry(true)
	}
}

func (s *session) allocConnection(c *connectionConfig) error {
	var err error

	s.conn, err = connection.New(&connection.Config{
		ID:           s.id,
		OnDisconnect: s.onDisconnect,
		Subscriber:   s.subscriber,
		Messenger:    s.messenger,
		Clean:        s.clean,
		ExpireIn:     s.expireIn,
		Username:     c.username,
		Auth:         c.auth,
		State:        c.state,
		Conn:         c.conn,
		Metric:       c.metric,
		KeepAlive:    c.keepAlive,
		SendQuota:    c.sendQuota,
		Version:      c.version,
	})

	if err == nil {
		s.connStop = &types.Once{}
	}

	return err
}

func (s *session) start() {
	s.wgStopped.Add(1)
	s.conn.Start()
}

func (s *session) stop(reason message.ReasonCode) *persistenceTypes.SessionState {
	s.connStop.Do(func() {
		if s.conn != nil {
			s.conn.Stop(reason)
		}
	})

	s.wgStopped.Wait()

	select {
	case <-s.timerChan:
	default:
		close(s.timerChan)
		s.timerWorker.Wait()
	}

	state := &persistenceTypes.SessionState{
		Timestamp: s.createdAt.Format(time.RFC3339),
		ExpireIn:  s.expireIn,
		Will: &persistenceTypes.SessionWill{
			Delay: s.willDelay,
		},
	}

	if s.will != nil {
		s.will.SetPacketID(0)
		sz, _ := s.will.Size()
		buf := make([]byte, sz)
		s.will.Encode(buf) // nolint: errcheck
		state.Will.Message = buf
	}

	return state
}

func (s *session) toOnline() bool {
	if atomic.CompareAndSwapUintptr(&s.isOnline, 0, 1) {
		select {
		case <-s.timerChan:
		default:
			close(s.timerChan)
		}

		return true
	}

	return false
}

func (s *session) runExpiry(will bool) {
	var expire *time.Duration

	// if meet will requirements point that
	if will && s.will != nil && s.willDelay >= 0 {
		expire = &s.willDelay
	} else {
		s.will = nil
	}

	//check if we set will delay before
	//if will delay bigger than session expiry interval set timer period to expireIn value
	//as will message (if presented) has to be published either session expiry or will delay (which done first)
	//if will delay less than session expiry set timer to will delay interval and store difference between
	//will delay and session expiry to let timer restart keep tick after will

	if s.expireIn != nil && *s.expireIn != 0 {
		// if will delay is set before and value less than expiration
		// then timer should fire 2 times
		if expire != nil && *expire < *s.expireIn {
			*s.expireIn = *s.expireIn - s.willDelay
		} else {
			expire = s.expireIn
			*s.expireIn = 0
		}
	}

	s.timerStartedAt = time.Now()
	s.timerWorker.Add(1)
	s.timerChan = make(chan struct{})
	s.timer = time.NewTimer(*expire * time.Second)
	go s.expiryWorker()
}

func (s *session) onDisconnect(p *connection.DisconnectParams) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
		}
	}()

	defer atomic.CompareAndSwapUintptr(&s.isOnline, 1, 0)
	defer s.wgStopped.Done()

	signalClose := true
	var shutdownReason exitReason

	defer func() {
		if signalClose {
			s.onClose(s.id, shutdownReason)
		}
	}()

	s.connStop.Do(func() {
		s.conn = nil
	})

	// valid willMsg pointer tells we have will message
	if p.Will && s.will != nil {
		// if session is clean send will regardless to will delay
		if s.clean || s.willDelay == 0 {
			s.messenger.Publish(s.will) // nolint: errcheck
			s.will = nil
		}
	}

	s.notifyDisconnect(s.id, !s.clean, 0)

	if s.clean {
		// session is clean. Signal upper layer to wipe it
		shutdownReason = exitReasonClean
	} else {
		// session is not clean thus more difficult case

		// persist state
		s.onPersist(s.id, p.State)

		// check if remaining subscriptions exists, expiry is presented and will delay not set to 0
		if s.expireIn == nil && s.willDelay == 0 {
			if !s.subscriber.HasSubscriptions() {
				// above false thus no remaining subscriptions, session does not expire
				// and does not require delayed will
				// signal upper layer to persist state and wipe this object
				shutdownReason = exitReasonShutdown
			} else {
				shutdownReason = exitReasonKeepSubscriber
			}
		} else if (s.expireIn != nil && *s.expireIn > 0) || s.willDelay > 0 {
			// session has either to expire or will delay or both set

			// do not signal upper layer about exit
			signalClose = false

			// new expiry value might be received upon disconnect message from the client
			if p.ExpireAt != nil {
				s.expireIn = p.ExpireAt
			}

			s.runExpiry(p.Will)
		}
	}
}

func (s *session) expiryWorker() {
	var shutdownReason exitReason

	defer func() {
		select {
		case <-s.timerChan:
		default:
			close(s.timerChan)
		}
		s.timerWorker.Done()

		if shutdownReason > 0 {
			s.onClose(s.id, shutdownReason)
		}
	}()

	for {
		select {
		case <-s.timer.C:
			// timer fired
			// 1. check if it requires to publish will
			if s.will != nil {
				// publish if exists and wipe state
				s.messenger.Publish(s.will) // nolint: errcheck
				s.will = nil
			}

			// 2. if expireIn is present this session has expiry set
			if s.expireIn != nil {
				// 2.a if value pointed by expireIn is non zero there is some time after will left wait
				if *s.expireIn != 0 {
					// restart timer and wait again
					val := *s.expireIn
					// clear value pointed by expireIn so when next fire comes we signal session is expired
					*s.expireIn = 0
					s.timer.Reset(val)
				} else {
					// session has expired. WIPE IT
					s.subscriber.Offline(true)
					shutdownReason = exitReasonExpired
					return
				}
			} else {
				// 2.b session has processed delayed will
				// if there is any subscriptions left tell upper layer to keep subscriber
				// otherwise completely shutdown the session
				if s.subscriber.HasSubscriptions() {
					shutdownReason = exitReasonKeepSubscriber
				} else {
					shutdownReason = exitReasonShutdown
				}

				return
			}
		case <-s.timerChan:
			if s.timer != nil {
				s.timer.Stop()
				elapsed := time.Now().Sub(s.timerStartedAt)
				if s.willDelay > 0 && (s.willDelay-elapsed) > 0 {
					s.willDelay = s.willDelay - elapsed
				}

				if s.expireIn != nil && *s.expireIn > 0 && (*s.expireIn-elapsed) > 0 {
					*s.expireIn = *s.expireIn - elapsed
				}
			}
			return
		}
	}
}

type properties struct {
	ExpireIn           *time.Duration
	WillDelay          time.Duration
	UserProperties     interface{}
	AuthData           []byte
	AuthMethod         string
	MaximumPacketSize  uint32
	ReceiveMaximum     uint16
	TopicAliasMaximum  uint16
	RequestResponse    bool
	RequestProblemInfo bool
}

func newProperties() *properties {
	return &properties{
		ExpireIn:           nil,
		WillDelay:          0,
		UserProperties:     nil,
		AuthData:           []byte{},
		AuthMethod:         "",
		MaximumPacketSize:  2684354565,
		ReceiveMaximum:     65535,
		TopicAliasMaximum:  0xFFFF,
		RequestResponse:    false,
		RequestProblemInfo: false,
	}
}
