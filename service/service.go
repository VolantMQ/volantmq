// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/juju/loggo"
	"github.com/troian/surgemq/buffer"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/session"
	"github.com/troian/surgemq/topics"
)

type (
	// OnCompleteFunc on complete
	OnCompleteFunc func(msg, ack message.Message, err error) error
	// OnPublishFunc on publish
	OnPublishFunc func(msg *message.PublishMessage) error
)

type stat struct {
	bytes int64
	mSgs  int64
}

func (s *stat) increment(n int64) {
	atomic.AddInt64(&s.bytes, n)
	atomic.AddInt64(&s.mSgs, 1)
}

// Config of service
type Config struct {
	Client bool

	// The number of seconds to keep the connection live if there's no data.
	// If not set then default to 5 mins.
	KeepAlive int

	// The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// The number of seconds to wait for any ACK messages before failing.
	// If not set then default to 20 seconds.
	AckTimeout int

	// The number of times to retry sending a packet if ACK is not received.
	// If no set then default to 3 retries.
	TimeoutRetries int

	// Session manager for tracking all the clients
	SessionMgr *session.Manager

	// Topics manager for all the client subscriptions
	TopicsMgr *topics.Manager

	// Network connection for this service
	Conn io.Closer

	ExitSignal chan uint64
}

// Type of service
type Type struct {
	Config

	// The ID of this service, it's not related to the Client ID, just a number that's
	// incremented for every new service.
	ID uint64

	// used during permissions check
	//clientID string

	//// Session manager for tracking all the clients
	//sessMgr *sessions.Manager

	//// Topics manager for all the client subscriptions
	//topicsMgr *topics.Manager

	// sess is the session object for this MQTT session. It keeps track session variables
	// such as ClientId, KeepAlive, Username, etc
	sess *session.Type

	// Wait for the various goroutines to finish starting and stopping
	wgStarted sync.WaitGroup
	wgStopped sync.WaitGroup

	disconnectByServer bool

	// writeMessage mutex - serializes writes to the outgoing buffer.
	wmu sync.Mutex

	// Whether this is service is closed or not.
	closed int64

	// Quit signal for determining when this service should end. If channel is closed,
	// then exit.
	done chan struct{}

	// Incoming data buffer. Bytes are read from the connection and put in here.
	in *buffer.Type

	// Outgoing data buffer. Bytes written here are in turn written out to the connection.
	out *buffer.Type

	// onPub is the method that gets added to the topic subscribers list by the
	// processSubscribe() method. When the server finishes the ack cycle for a
	// PUBLISH message, it will call the subscriber, which is this method.
	//
	// For the server, when this method is called, it means there's a message that
	// should be published to the client on the other end of this connection. So we
	// will call publish() to send the message.
	onPub OnPublishFunc

	inStat  stat
	outStat stat

	inTmp  []byte
	outTmp []byte

	subs  []interface{}
	qoss  []byte
	rmSgs []*message.PublishMessage
}

var appLog loggo.Logger

func init() {
	appLog = loggo.GetLogger("mq.service")
	appLog.SetLogLevel(loggo.INFO)
}

// NewService alloc
func NewService(config Config) (*Type, error) {
	svc := &Type{
		Config:             config,
		disconnectByServer: false,
	}

	return svc, nil
}

// Start service
func (s *Type) Start(inStat, outStat int64) error {
	var err error

	s.closed = 0
	s.inStat.increment(inStat)
	s.outStat.increment(outStat)

	// Create the incoming ring buffer
	s.in, err = buffer.New(buffer.DefaultBufferSize)
	if err != nil {
		return err
	}

	// Create the outgoing ring buffer
	s.out, err = buffer.New(buffer.DefaultBufferSize)
	if err != nil {
		return err
	}

	// If this is a server
	if !s.Client {
		// Create the onPublishFunc so it can be used for published messages
		s.onPub = func(msg *message.PublishMessage) error {
			if err := s.Publish(msg, nil); err != nil {
				appLog.Errorf("service/onPublish: Error publishing message: %v", err)
				return err
			}

			return nil
		}

		// If this is a recovered session, then add any topics it subscribed before
		topics, err := s.sess.Topics()
		if err != nil {
			return err
		}

		for t, q := range *topics {
			if _, err = s.TopicsMgr.Subscribe(t, q, &s.onPub); err != nil {
				// error happened, un-subscribe to previously subscribed topics
				for iT := range *topics {
					if iT != t {
						if err2 := s.TopicsMgr.UnSubscribe(iT, &s.onPub); err2 != nil {
							appLog.Errorf(err2.Error())
						}
					} else {
						break
					}
				}

				return err
			}
		}
	}

	// Processor is responsible for reading messages out of the buffer and processing
	// them accordingly.
	s.wgStarted.Add(1)
	s.wgStopped.Add(1)
	go s.processor()

	// Receiver is responsible for reading from the connection and putting data into
	// a buffer.
	s.wgStarted.Add(1)
	s.wgStopped.Add(1)
	go s.receiver()

	// Sender is responsible for writing data in the buffer into the connection.
	s.wgStarted.Add(1)
	s.wgStopped.Add(1)
	go s.sender()

	// Wait for all the goroutines to start before returning
	s.wgStarted.Wait()

	return nil
}

// Stop service
func (s *Type) Stop() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			appLog.Errorf("[%s] recovering from panic: %v", s.CID(), r)
		}
	}()

	if doIT := atomic.CompareAndSwapInt64(&s.closed, 0, 1); !doIT {
		return
	}

	// Close quit channel, effectively telling all the goroutines it's time to quit
	if s.done != nil {
		appLog.Debugf("[%s] closing done channel", s.CID())
		close(s.done)
	}

	// Close the network connection
	if s.Conn != nil {
		appLog.Debugf("[%s] closing network connection", s.CID())
		if err := s.Conn.Close(); err != nil {
			appLog.Errorf("close connection [%s] error: %s", s.CID(), err.Error())
		}
	}

	if err := s.in.Close(); err != nil {
		appLog.Errorf("[%s] close input buffer error: %s", s.CID(), err.Error())
	}

	if err := s.out.Close(); err != nil {
		appLog.Errorf("[%s] close output buffer error: %s", s.CID(), err.Error())
	}

	// Wait for all the goroutines to stop.
	s.wgStopped.Wait()

	appLog.Debugf("[%s] received %d bytes in %d messages.", s.CID(), s.inStat.bytes, s.inStat.mSgs)
	appLog.Debugf("[%s] sent %d bytes in %d messages.", s.CID(), s.outStat.bytes, s.outStat.mSgs)

	if !s.Client {
		if s.sess != nil {
			// UnSubscribe from all the topics for this client, only for the server side though
			if topics, err := s.sess.Topics(); err != nil {
				appLog.Errorf("[%s]: %v", s.CID(), err)
			} else {
				for t := range *topics {
					if err := s.TopicsMgr.UnSubscribe(t, &s.onPub); err != nil {
						appLog.Errorf("[%s]: error unsubscribing topic %q: %v", s.CID(), t, err)
					}
				}
			}

			// Publish will message if WillFlag is set
			if s.sess.Will != nil {
				appLog.Infof("[%s] service/stop: connection unexpectedly closed. Sending Will.", s.CID())
				s.onPublish(s.sess.Will) // nolint: errcheck
			}
		}
	} else {
		// Remove the client topics manager
		topics.UnRegister(s.sess.ID())
	}

	// Remove the session from sessions store if it's suppose to be clean session
	if s.sess != nil && s.sess.IsClean() {
		s.SessionMgr.Del(s.sess.ID())
	}

	s.Conn = nil
	s.in = nil
	s.out = nil
}

// Publish send message
func (s *Type) Publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	if _, err := s.writeMessage(msg); err != nil {
		return fmt.Errorf("[%s] couldn't send [%s] message: %v", s.CID(), msg.Name(), err)
	}

	switch msg.QoS() {
	case message.QosAtMostOnce:
		if onComplete != nil {
			return onComplete(msg, nil, nil)
		}

		return nil
	case message.QosAtLeastOnce:
		return s.sess.Pub1ack.Wait(msg, onComplete)
	case message.QosExactlyOnce:
		return s.sess.Pub2out.Wait(msg, onComplete)
	}

	return nil
}

// Subscribe send message
func (s *Type) Subscribe(msg *message.SubscribeMessage, onComplete OnCompleteFunc, onPublish OnPublishFunc) error {
	if onPublish == nil {
		return message.ErrOnPublishNil
	}

	_, err := s.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("[%s] couldn't send [%s] message: %v", s.CID(), msg.Name(), err)
	}

	var onc OnCompleteFunc = func(msg, ack message.Message, err error) error {
		onComplete := onComplete
		onPublish := onPublish

		if err != nil {
			if onComplete != nil {
				return onComplete(msg, ack, err)
			}
			return err
		}

		sub, ok := msg.(*message.SubscribeMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, message.ErrInvalidUnSubscribe)
			}
			return nil
		}

		subAck, ok := ack.(*message.SubAckMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, message.ErrInvalidUnSubAck)
			}
			return nil
		}

		if sub.PacketID() != subAck.PacketID() {
			if onComplete != nil {
				return onComplete(msg, ack, message.ErrPackedIDNotMatched)
			}
			return nil
		}

		retCodes := subAck.ReturnCodes()
		topics := sub.Topics()

		if len(topics) != len(retCodes) {
			if onComplete != nil {
				fmtMsg := "Incorrect number of return codes received. Expecting %d, got %d"
				return onComplete(msg, ack, fmt.Errorf(fmtMsg, len(topics), len(retCodes)))
			}
			return nil
		}

		var err2 error = nil

		for i, t := range topics {
			c := retCodes[i]

			if c == message.QosFailure {
				err2 = fmt.Errorf("Failed to subscribe to '%s'\n%v", string(t), err2)
			} else {
				s.sess.AddTopic(string(t), c) // nolint: errcheck
				_, err := s.TopicsMgr.Subscribe(string(t), c, &onPublish)
				if err != nil {
					err2 = fmt.Errorf("Failed to subscribe to '%s' (%v)\n%v", string(t), err, err2)
				}
			}
		}

		if onComplete != nil {
			return onComplete(msg, ack, err2)
		}

		return err2
	}

	return s.sess.SubAck.Wait(msg, onc)
}

// UnSubscribe send message
func (s *Type) UnSubscribe(msg *message.UnSubscribeMessage, onComplete OnCompleteFunc) error {
	if _, err := s.writeMessage(msg); err != nil {
		return fmt.Errorf("[%s] couldn't send [%s] message: %v", s.CID(), msg.Name(), err)
	}

	var onc OnCompleteFunc = func(msg, ack message.Message, err error) error {
		onComplete := onComplete

		if err != nil {
			if onComplete != nil {
				return onComplete(msg, ack, err)
			}
			return err
		}

		unSub, ok := msg.(*message.UnSubscribeMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, message.ErrInvalidUnSubscribe)
			}
			return nil
		}

		unSubAck, ok := ack.(*message.UnSubAckMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, message.ErrInvalidUnSubAck)
			}
			return nil
		}

		if unSub.PacketID() != unSubAck.PacketID() {
			if onComplete != nil {
				return onComplete(msg, ack, message.ErrPackedIDNotMatched)
			}
			return nil
		}

		var err2 error = nil

		for _, tb := range unSub.Topics() {
			// Remove all subscribers, which basically it's just this client, since
			// each client has it's own topic tree.
			err := s.TopicsMgr.UnSubscribe(string(tb), nil)
			if err != nil {
				err2 = fmt.Errorf("%v\n%v", err2, err)
			}

			s.sess.RemoveTopic(string(tb)) // nolint: errcheck
		}

		if onComplete != nil {
			return onComplete(msg, ack, err2)
		}

		return err2
	}

	return s.sess.UnSubAck.Wait(msg, onc)
}

// Ping service
func (s *Type) Ping(onComplete OnCompleteFunc) error {
	msg := message.NewPingReqMessage()

	_, err := s.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("[%s] couldn't send [%s] message: %v", s.CID(), msg.Name(), err)
	}

	return s.sess.PingAck.Wait(msg, onComplete)
}

// IsDone is service finished
func (s *Type) IsDone() bool {
	select {
	case <-s.done:
		return true

	default:
	}

	return false
}

// CID get service id
func (s *Type) CID() string {
	return fmt.Sprintf("%d/%s", s.ID, s.sess.ID())
}

// SetSession set service session
func (s *Type) SetSession(ses *session.Type) {
	s.sess = ses
}
