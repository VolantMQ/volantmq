// Copyright (c) 2014 The VolantMQ Authors. All rights reserved.
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

package connection

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/plugin/persistence"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/transport"
	"github.com/VolantMQ/volantmq/types"
	"go.uber.org/zap"
)

type state int

const (
	stateConnecting state = iota
	stateAuth
	stateConnected
	stateReAuth
	stateDisconnected
	stateConnectFailed
)

// nolint: golint
var (
	ErrOverflow    = errors.New("session: overflow")
	ErrPersistence = errors.New("session: error during persistence restore")
)

var expectedPacketType = map[state]map[mqttp.Type]bool{
	stateConnecting: {mqttp.CONNECT: true},
	stateAuth: {
		mqttp.AUTH:       true,
		mqttp.DISCONNECT: true,
	},
	stateConnected: {
		mqttp.PUBLISH:     true,
		mqttp.PUBACK:      true,
		mqttp.PUBREC:      true,
		mqttp.PUBREL:      true,
		mqttp.PUBCOMP:     true,
		mqttp.SUBSCRIBE:   true,
		mqttp.SUBACK:      true,
		mqttp.UNSUBSCRIBE: true,
		mqttp.UNSUBACK:    true,
		mqttp.PINGREQ:     true,
		mqttp.AUTH:        true,
		mqttp.DISCONNECT:  true,
	},
	stateReAuth: {
		mqttp.PUBLISH:     true,
		mqttp.PUBACK:      true,
		mqttp.PUBREC:      true,
		mqttp.PUBREL:      true,
		mqttp.PUBCOMP:     true,
		mqttp.SUBSCRIBE:   true,
		mqttp.SUBACK:      true,
		mqttp.UNSUBSCRIBE: true,
		mqttp.UNSUBACK:    true,
		mqttp.PINGREQ:     true,
		mqttp.AUTH:        true,
		mqttp.DISCONNECT:  true,
	},
}

func (s state) desc() string {
	switch s {
	case stateConnecting:
		return "CONNECTING"
	case stateAuth:
		return "AUTH"
	case stateConnected:
		return "CONNECTED"
	case stateReAuth:
		return "RE-AUTH"
	case stateDisconnected:
		return "DISCONNECTED"
	default:
		return "CONNECT_FAILED"
	}
}

type signalConnectionClose func(error) bool
type signalIncoming func(mqttp.IFace) error

// DisconnectParams session state when stopped
type DisconnectParams struct {
	Reason  mqttp.ReasonCode
	Packets persistence.PersistedPackets
}

// Callbacks provided by sessions manager to signal session state
type Callbacks struct {
	// OnStop called when session stopped net connection and should be either suspended or deleted
	OnStop func(string, bool)
}

// WillConfig configures session for will messages
type WillConfig struct {
	Topic   string
	Message []byte
	Retain  bool
	QoS     mqttp.QosType
}

// AuthParams ...
type AuthParams struct {
	AuthMethod string
	AuthData   []byte
	Reason     mqttp.ReasonCode
}

// ConnectParams ...
type ConnectParams struct {
	AuthParams
	ID              string
	Error           error
	ExpireIn        *uint32
	Will            *mqttp.Publish
	Username        []byte
	Password        []byte
	MaxTxPacketSize uint32
	SendQuota       uint16
	KeepAlive       uint16
	IDGen           bool
	CleanStart      bool
	Durable         bool
	Version         mqttp.ProtocolVersion
}

// SessionCallbacks ...
type SessionCallbacks interface {
	SignalPublish(*mqttp.Publish) error
	SignalSubscribe(*mqttp.Subscribe) (mqttp.IFace, error)
	SignalUnSubscribe(*mqttp.UnSubscribe) (mqttp.IFace, error)
	SignalDisconnect(*mqttp.Disconnect) (mqttp.IFace, error)
	SignalOnline()
	SignalOffline()
	SignalConnectionClose(DisconnectParams)
}

// impl of the connection
type impl struct {
	SessionCallbacks
	id               string
	conn             transport.Conn
	metric           systree.PacketsMetric
	signalAuth       OnAuthCb
	onConnClose      func(error)
	callStop         func(error) bool
	tx               *writer
	rx               *reader
	quit             chan struct{}
	connect          chan interface{}
	onStart          types.Once
	onConnDisconnect types.OnceWait
	onStop           types.Once
	started          sync.WaitGroup
	log              *zap.SugaredLogger
	pubIn            ackQueue
	authMethod       string
	connectProcessed uint32
	rxQuota          int32
	state            state
	maxRxTopicAlias  uint16
	version          mqttp.ProtocolVersion
	retainAvailable  bool
}

type unacknowledged struct {
	mqttp.IFace
}

type sizeAble interface {
	Size() (int, error)
}

type baseAPI interface {
	Stop(error) bool
}

// Initial ...
type Initial interface {
	baseAPI
	Accept() (chan interface{}, error)
	Send(mqttp.IFace) error
	Acknowledge(p *mqttp.ConnAck, opts ...Option) bool
	Session() Session
}

// Session ...
type Session interface {
	baseAPI
	Publish(string, *mqttp.Publish)
	SetOptions(opts ...Option) error
}

var _ Initial = (*impl)(nil)
var _ Session = (*impl)(nil)

// New allocate new connection object
func New(opts ...Option) Initial {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
		}
	}()

	s := &impl{
		state: stateConnecting,
		quit:  make(chan struct{}),
		tx:    newWriter(),
		rx:    newReader(),
	}

	s.log = configuration.GetLogger().Named("connection")
	s.onConnClose = s.onConnectionCloseStage1
	s.callStop = s.stopNonAck

	s.tx.setOptions(
		wrLog(s.log),
		wrOnConnClose(s.onConnectionClose),
	)

	s.rx.setOptions(
		rdLog(s.log),
		rdOnConnClose(s.onConnectionClose),
		rdProcessIncoming(s.processIncoming),
	)

	for _, opt := range opts {
		opt(s)
	}

	s.started.Add(1)
	s.pubIn.onRelease = s.onReleaseIn

	return s
}

// Accept start handling incoming connection
func (s *impl) Accept() (chan interface{}, error) {
	var err error

	defer func() {
		if r := recover(); r != nil {
			s.log.Panic(r)
		}
	}()
	defer func() {
		if err != nil {
			close(s.connect)
			s.conn.Close()
		}
	}()
	s.connect = make(chan interface{})
	s.rx.setOptions(
		rdConnect(s.connect),
	)

	s.rx.connection()

	return s.connect, nil
}

// Session object
func (s *impl) Session() Session {
	return s
}

// Send packet to connection
func (s *impl) Send(pkt mqttp.IFace) (err error) {
	defer func() {
		if err != nil {
			close(s.connect)
		}
	}()

	if pkt.Type() == mqttp.AUTH {
		s.state = stateAuth
	}

	s.tx.sendGeneric(pkt)

	s.rx.connection()

	return nil
}

// Acknowledge incoming connection
func (s *impl) Acknowledge(p *mqttp.ConnAck, opts ...Option) bool {
	ack := true
	s.conn.SetReadDeadline(time.Time{})

	close(s.connect)

	if p.ReturnCode() == mqttp.CodeSuccess {
		s.state = stateConnected

		for _, opt := range opts {
			opt(s)
		}
	} else {
		s.state = stateConnectFailed
		ack = false
	}

	buf, _ := mqttp.Encode(p)
	bufs := net.Buffers{buf}

	if _, err := bufs.WriteTo(s.conn); err != nil {
		ack = false
	} else {
		s.metric.Sent(p.Type())
	}

	if !ack {
		s.stopNonAck(nil)
	} else {
		s.onConnClose = s.onConnectionCloseStage2
		s.callStop = s.onConnectionClose
		s.SignalOnline()
		s.rx.run()
		s.tx.start(true)
	}

	return ack
}

// Stop connection. Function assumed to be invoked once server about to either shutdown, disconnect
// or session is being replaced
// Effective only first invoke
func (s *impl) Stop(reason error) bool {
	return s.callStop(reason)
}

func (s *impl) stopNonAck(reason error) bool {
	s.tx.start(false)
	return s.onConnectionClose(reason)
}

// Publish ...
func (s *impl) Publish(id string, pkt *mqttp.Publish) {
	s.tx.send(pkt)
}

func genClientID() string {
	b := make([]byte, 15)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}

	return base64.URLEncoding.EncodeToString(b)
}

func (s *impl) onConnect(pkt *mqttp.Connect) (mqttp.IFace, error) {
	if atomic.CompareAndSwapUint32(&s.connectProcessed, 0, 1) {
		id := string(pkt.ClientID())
		idGen := false
		if len(id) == 0 {
			idGen = true
			id = genClientID()
		}

		s.id = id

		params := &ConnectParams{
			ID:         id,
			IDGen:      idGen,
			Will:       pkt.Will(),
			KeepAlive:  pkt.KeepAlive(),
			Version:    pkt.Version(),
			CleanStart: pkt.IsClean(),
			Durable:    true,
		}

		params.Username, params.Password = pkt.Credentials()
		s.version = params.Version

		s.readConnProperties(pkt, params)

		s.tx.setOptions(
			wrVersion(pkt.Version()),
			wrID(id),
		)

		s.rx.setOptions(
			rdVersion(pkt.Version()),
		)

		// MQTT v5 has different meaning of clean comparing to MQTT v3
		//  - v3: if session is clean it is clean start and session lasts when Network connection closed
		//  - v5: clean only means "clean start" and sessions lasts on connection close on if expire propery
		//          exists and set to 0
		if (params.Version <= mqttp.ProtocolV311 && params.CleanStart) ||
			(params.Version >= mqttp.ProtocolV50 && params.ExpireIn != nil && *params.ExpireIn == 0) {
			params.Durable = false
		}

		s.connect <- params
		return nil, nil
	}

	// It's protocol error to send CONNECT packet more than once
	return nil, mqttp.CodeProtocolError
}

func (s *impl) onAuth(pkt *mqttp.Auth) (mqttp.IFace, error) {
	// AUTH packets are allowed for v5.0 only
	if s.version < mqttp.ProtocolV50 {
		return nil, mqttp.CodeRefusedServerUnavailable
	}

	reason := pkt.ReasonCode()

	// Client must not send AUTH packets before server has requested it
	// during auth or re-auth Client must respond only AUTH with CodeContinueAuthentication
	// if connection is being established Client must send AUTH only with CodeReAuthenticate
	if (s.state == stateConnecting) ||
		((s.state == stateAuth || s.state == stateReAuth) && (reason != mqttp.CodeContinueAuthentication)) ||
		((s.state == stateConnected) && reason != (mqttp.CodeReAuthenticate)) {
		return nil, mqttp.CodeProtocolError
	}

	params := AuthParams{
		Reason: reason,
	}

	// [MQTT-3.15.2.2.2]
	if prop := pkt.PropertyGet(mqttp.PropertyAuthMethod); prop != nil {
		if val, e := prop.AsString(); e == nil {
			params.AuthMethod = val
		}
	}

	// AUTH packet must provide AuthMethod property
	if len(params.AuthMethod) == 0 {
		return nil, mqttp.CodeProtocolError
	}

	// [MQTT-4.12.0-7] - If the Client does not include an Authentication Method in the CONNECT,
	//                   the Client MUST NOT send an AUTH packet to the Server
	// [MQTT-4.12.1-1] - The Client MUST set the Authentication Method to the same value as
	//                   the Authentication Method originally used to authenticate the Network Connection
	if len(s.authMethod) == 0 || s.authMethod != params.AuthMethod {
		return nil, mqttp.CodeProtocolError
	}

	// [MQTT-3.15.2.2.3]
	if prop := pkt.PropertyGet(mqttp.PropertyAuthData); prop != nil {
		if val, e := prop.AsBinary(); e == nil {
			params.AuthData = val
		}
	}

	if s.state == stateConnecting || s.state == stateAuth {
		s.connect <- params
		return nil, nil
	}

	return s.signalAuth(s.id, &params)
}

func (s *impl) readConnProperties(req *mqttp.Connect, params *ConnectParams) {
	if s.version < mqttp.ProtocolV50 {
		return
	}

	// [MQTT-3.1.2.11.2]
	if prop := req.PropertyGet(mqttp.PropertySessionExpiryInterval); prop != nil {
		if val, e := prop.AsInt(); e == nil {
			params.ExpireIn = &val
		}
	}

	// [MQTT-3.1.2.11.4]
	if prop := req.PropertyGet(mqttp.PropertyReceiveMaximum); prop != nil {
		if val, e := prop.AsShort(); e == nil {
			s.tx.setOptions(
				wrQuota(int32(val)),
			)
			params.SendQuota = val
		}
	}

	// [MQTT-3.1.2.11.5]
	if prop := req.PropertyGet(mqttp.PropertyMaximumPacketSize); prop != nil {
		if val, e := prop.AsInt(); e == nil {
			s.tx.setOptions(
				wrMaxPacketSize(val),
			)
		}
	}

	// [MQTT-3.1.2.11.6]
	if prop := req.PropertyGet(mqttp.PropertyTopicAliasMaximum); prop != nil {
		if val, e := prop.AsShort(); e == nil {
			s.tx.setOptions(
				wrTopicAliasMax(val),
			)
		}
	}

	// [MQTT-3.1.2.11.10]
	if prop := req.PropertyGet(mqttp.PropertyAuthMethod); prop != nil {
		if val, e := prop.AsString(); e == nil {
			params.AuthMethod = val
			s.authMethod = val
		}
	}

	// [MQTT-3.1.2.11.11]
	if prop := req.PropertyGet(mqttp.PropertyAuthData); prop != nil {
		if len(params.AuthMethod) == 0 {
			params.Error = mqttp.CodeProtocolError
			return
		}
		if val, e := prop.AsBinary(); e == nil {
			params.AuthData = val
		}
	}

	return
}

func (s *impl) processIncoming(p mqttp.IFace) error {
	var err error
	var resp mqttp.IFace

	// [MQTT-3.1.2-33] - If a Client sets an Authentication Method in the CONNECT,
	//                   the Client MUST NOT send any packets other than AUTH or DISCONNECT packets
	//                   until it has received a CONNACK packet
	if _, ok := expectedPacketType[s.state][p.Type()]; !ok {
		s.log.Debug("Unexpected packet for current state",
			zap.String("ClientID", s.id),
			zap.String("state", s.state.desc()),
			zap.String("packet", p.Type().Name()))
		return mqttp.CodeProtocolError
	}

	switch pkt := p.(type) {
	case *mqttp.Connect:
		resp, err = s.onConnect(pkt)
	case *mqttp.Auth:
		resp, err = s.onAuth(pkt)
	case *mqttp.Publish:
		resp, err = s.onPublish(pkt)
	case *mqttp.Ack:
		resp, err = s.onAck(pkt)
	case *mqttp.Subscribe:
		resp, err = s.SignalSubscribe(pkt)
	case *mqttp.UnSubscribe:
		resp, err = s.SignalUnSubscribe(pkt)
	case *mqttp.PingReq:
		resp = mqttp.NewPingResp(s.version)
	case *mqttp.Disconnect:
		s.onStop.Do(func() {
			s.SignalOffline()
			s.tx.stop()
		})

		resp, err = s.SignalDisconnect(pkt)
	}

	if resp != nil {
		s.tx.send(resp)
	}

	return err
}

// forward PUBLISH message to topics manager which takes care about subscribers
func (s *impl) publishToTopic(p *mqttp.Publish) error {
	// v5.0
	// If the Server included Retain Available in its CONNACK response to a Client with its value set to 0 and it
	// receives a PUBLISH packet with the RETAIN flag is set to 1, then it uses the DISCONNECT Reason
	// Code of 0x9A (Retain not supported) as described in section 4.13.
	if s.version >= mqttp.ProtocolV50 {
		// [MQTT-3.3.2.3.4]
		if prop := p.PropertyGet(mqttp.PropertyTopicAlias); prop != nil {
			if val, err := prop.AsShort(); err == nil {
				if len(p.Topic()) != 0 {
					// renew alias with new topic
					s.rx.topicAlias[val] = p.Topic()
				} else {
					if topic, kk := s.rx.topicAlias[val]; kk {
						// do not check for error as topic has been validated when arrived
						if err = p.SetTopic(topic); err != nil {
							s.log.Error("publish to topic",
								zap.String("ClientID", s.id),
								zap.String("topic", topic),
								zap.Error(err))
						}
					} else {
						return mqttp.CodeInvalidTopicAlias
					}
				}
			} else {
				return mqttp.CodeInvalidTopicAlias
			}
		}

		// [MQTT-3.3.2.3.3]
		if prop := p.PropertyGet(mqttp.PropertyPublicationExpiry); prop != nil {
			if val, err := prop.AsInt(); err == nil {
				s.log.Debug("Set pub expiration",
					zap.String("ClientID", s.id),
					zap.Duration("val", time.Duration(val)*time.Second))
				p.SetExpireAt(time.Now().Add(time.Duration(val) * time.Second))
			} else {
				return err
			}
		}
	}

	return s.SignalPublish(p)
}

// onReleaseIn ack process for incoming messages
func (s *impl) onReleaseIn(o, n mqttp.IFace) {
	switch p := o.(type) {
	case *mqttp.Publish:
		s.publishToTopic(p)
	}
}

func (s *impl) onConnectionCloseStage1(status error) {
	// shutdown quit channel tells all routines finita la commedia
	close(s.quit)

	s.conn.SetReadDeadline(time.Time{})

	select {
	case <-s.connect:
	default:
		close(s.connect)
	}

	s.tx.stop()

	s.rx.shutdown()
	s.tx.shutdown()
	s.tx = nil
	s.rx = nil

	s.state = stateDisconnected

	if err := s.conn.Close(); err != nil {
		s.log.Warn("close connection", zap.String("clientId", s.id), zap.Error(err))
	}

	s.conn = nil
}

func (s *impl) onConnectionCloseStage2(status error) {
	// shutdown quit channel tells all routines finita la commedia
	close(s.quit)

	var err error
	s.conn.SetReadDeadline(time.Time{})

	// clean up transmitter to allow send disconnect command to client if needed
	s.onStop.Do(func() {
		// gracefully shutdown receiver by setting some small ReadDeadline
		s.conn.SetReadDeadline(time.Now().Add(time.Microsecond))
		s.rx.shutdown()

		s.SignalOffline()

		s.tx.stop()
	})

	if reason, ok := status.(mqttp.ReasonCode); ok &&
		reason != mqttp.CodeSuccess && s.version >= mqttp.ProtocolV50 {
		// server wants to tell client disconnect reason
		pkt := mqttp.NewDisconnect(s.version)
		pkt.SetReasonCode(reason)

		var buf []byte
		if buf, err = mqttp.Encode(pkt); err != nil {
			s.log.Error("encode disconnect packet", zap.String("ClientID", s.id), zap.Error(err))
		} else {
			var written int
			if written, err = s.conn.Write(buf); written != len(buf) {
				s.log.Error("write disconnect message",
					zap.String("clientId", s.id),
					zap.Int("packet size", len(buf)),
					zap.Int("written", written))
			} else if err != nil {
				s.log.Debug("write disconnect message",
					zap.String("clientId", s.id),
					zap.Error(err))
			}
		}
	}

	if err = s.conn.Close(); err != nil {
		s.log.Error("close connection", zap.String("clientId", s.id), zap.Error(err))
	}

	s.tx.shutdown()
	s.conn = nil

	params := DisconnectParams{
		Packets: s.tx.getQueuedPackets(),
		Reason:  mqttp.CodeSuccess,
	}

	s.tx = nil
	s.rx = nil

	if rc, ok := err.(mqttp.ReasonCode); ok {
		params.Reason = rc
	}

	s.SignalConnectionClose(params)

	s.state = stateDisconnected

	s.pubIn.messages.Range(func(k, v interface{}) bool {
		s.pubIn.messages.Delete(k)
		return true
	})
}

func (s *impl) onConnectionClose(status error) bool {
	return s.onConnDisconnect.Do(func() {
		s.onConnClose(status)
	})
}

// onPublish invoked when server receives PUBLISH message from remote
// On QoS == 0, we should just take the next step, no ack required
// On QoS == 1, send back PUBACK, then take the next step
// On QoS == 2, we need to put it in the ack queue, send back PUBREC
func (s *impl) onPublish(pkt *mqttp.Publish) (mqttp.IFace, error) {
	// check for topic access
	var err error
	reason := mqttp.CodeSuccess

	if s.version >= mqttp.ProtocolV50 {
		if !s.retainAvailable && pkt.Retain() {
			return nil, mqttp.CodeRetainNotSupported
		}

		if prop := pkt.PropertyGet(mqttp.PropertyTopicAlias); prop != nil {
			if val, ok := prop.AsShort(); ok == nil && (val == 0 || val > s.maxRxTopicAlias) {
				return nil, mqttp.CodeInvalidTopicAlias
			}
		}
	}

	var resp mqttp.IFace

	// This case is for V5.0 actually as ack messages may return status.
	// To deal with V3.1.1 two ways left:
	//   - ignore the message but send acks
	//   - return error leading to disconnect
	// TODO: publish permissions

	switch pkt.QoS() {
	case mqttp.QoS2:
		if s.rxQuota == 0 {
			err = mqttp.CodeReceiveMaximumExceeded
		} else {
			s.rxQuota--
			r := mqttp.NewPubRec(s.version)
			id, _ := pkt.ID()

			r.SetPacketID(id)

			resp = r

			// [MQTT-4.3.3-9]
			// store incoming QoS 2 message before sending PUBREC as theoretically PUBREL
			// might come before store in case message store done after write PUBREC
			if reason < mqttp.CodeUnspecifiedError {
				s.pubIn.store(pkt)
			}

			r.SetReason(mqttp.CodeSuccess)
		}
	case mqttp.QoS1:
		if s.rxQuota == 0 {
			err = mqttp.CodeReceiveMaximumExceeded
			break
		}

		r := mqttp.NewPubAck(s.version)

		id, _ := pkt.ID()

		r.SetPacketID(id)
		r.SetReason(reason)
		resp = r
		fallthrough
	case mqttp.QoS0: // QoS 0
		// [MQTT-4.3.1]
		// [MQTT-4.3.2-4]
		// TODO(troian): ignore if publish permissions not validated
		if err = s.publishToTopic(pkt); err != nil {
			s.log.Error("Couldn't publish message",
				zap.String("ClientID", s.id),
				zap.Uint8("QoS", uint8(pkt.QoS())),
				zap.Error(err))
		}
	}

	return resp, err
}

// onAck handle ack acknowledgment received from remote
func (s *impl) onAck(pkt mqttp.IFace) (mqttp.IFace, error) {
	var resp mqttp.IFace
	switch mIn := pkt.(type) {
	case *mqttp.Ack:
		switch pkt.Type() {
		case mqttp.PUBACK:
			// remote acknowledged PUBLISH QoS 1 message sent by this server
			s.tx.pubOut.release(pkt)
		case mqttp.PUBREC:
			// remote received PUBLISH message sent by this server
			s.tx.pubOut.release(pkt)

			discard := false

			id, _ := pkt.ID()

			if s.version == mqttp.ProtocolV50 && mIn.Reason() >= mqttp.CodeUnspecifiedError {
				// v5.0 [MQTT-4.9]
				s.tx.releaseID(id)
				discard = true
			}

			if !discard {
				resp, _ = mqttp.New(s.version, mqttp.PUBREL)
				r, _ := resp.(*mqttp.Ack)

				r.SetPacketID(id)

				// 2. Put PUBREL into ack queue
				// Do it before writing into network as theoretically response may come
				// faster than put into queue
				s.tx.pubOut.store(resp)
			}
		case mqttp.PUBREL:
			// Remote has released PUBLISH
			resp, _ = mqttp.New(s.version, mqttp.PUBCOMP)
			r, _ := resp.(*mqttp.Ack)

			id, _ := pkt.ID()
			r.SetPacketID(id)

			s.pubIn.release(pkt)
			s.rxQuota++
		case mqttp.PUBCOMP:
			// PUBREL message has been acknowledged, release from queue
			s.tx.pubOut.release(pkt)
		default:
			s.log.Error("Unsupported ack message type",
				zap.String("ClientID", s.id),
				zap.String("type", pkt.Type().Name()))
		}
	}

	return resp, nil
}
