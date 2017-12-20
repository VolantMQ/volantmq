package connection

import (
	"sync/atomic"

	"github.com/VolantMQ/volantmq/auth"
	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/topics/types"
	"go.uber.org/zap"
)

func (s *Type) txShutdown() {
	atomic.StoreUint32(&s.txRunning, 2)
	s.txTimer.Stop()
	s.txWg.Wait()

	select {
	case <-s.txAvailable:
	default:
		close(s.txAvailable)
	}
}

func (s *Type) rxShutdown() {
	s.rxWg.Wait()

	if s.keepAlive > 0 {
		s.keepAliveTimer.Stop()
		s.keepAliveTimer = nil
	}
}

func (s *Type) onConnectionClose(will bool, err error) {
	s.onConnDisconnect.Do(func() {
		// make sure connection has been started before proceeding to any shutdown procedures
		s.started.Wait()

		// shutdown quit channel tells all routines finita la commedia
		close(s.quit)
		if e := s.EventPoll.Stop(s.Desc); e != nil {
			s.log.Error("remove receiver from netpoll", zap.String("ClientID", s.ID), zap.Error(e))
		}
		// clean up transmitter to allow send disconnect command to client if needed
		s.txShutdown()

		// put subscriber in offline mode
		s.Subscriber.Offline(s.KillOnDisconnect)

		if err != nil && s.Version >= packet.ProtocolV50 {
			// server wants to tell client disconnect reason
			reason, _ := err.(packet.ReasonCode)
			p, _ := packet.New(s.Version, packet.DISCONNECT)
			pkt, _ := p.(*packet.Disconnect)
			pkt.SetReasonCode(reason)

			var buf []byte
			buf, err = packet.Encode(pkt)
			if err != nil {
				s.log.Error("encode disconnect packet", zap.String("ClientID", s.ID), zap.Error(err))
			} else {
				if _, err = s.Conn.Write(buf); err != nil {
					s.log.Error("Couldn't write disconnect message", zap.String("ClientID", s.ID), zap.Error(err))
				}
			}
		}

		if err = s.Conn.Close(); err != nil {
			s.log.Error("close connection", zap.String("ClientID", s.ID), zap.Error(err))
		}

		s.rxShutdown()

		// [MQTT-3.3.1-7]
		// Discard retained messages with QoS 0
		s.retained.lock.Lock()
		//for _, m := range s.retained.list {
		//	s.topics.Retain(m) // nolint: errcheck
		//}
		s.retained.list = []*packet.Publish{}
		s.retained.lock.Unlock()
		s.Conn = nil

		params := &DisconnectParams{
			Will:     will,
			ExpireAt: s.ExpireIn,
			Desc:     s.Desc,
			Reason:   packet.CodeSuccess,
		}

		if rc, ok := err.(packet.ReasonCode); ok {
			params.Reason = rc
		}

		if !s.KillOnDisconnect {
			s.persist()
		}

		s.OnDisconnect(params)
	})
}

// onPublish invoked when server receives PUBLISH message from remote
// On QoS == 0, we should just take the next step, no ack required
// On QoS == 1, send back PUBACK, then take the next step
// On QoS == 2, we need to put it in the ack queue, send back PUBREC
func (s *Type) onPublish(pkt *packet.Publish) (packet.Provider, error) {
	// check for topic access
	var err error
	reason := packet.CodeSuccess

	if err = s.preProcessPublish(pkt); err != nil {
		return nil, err
	}

	var resp packet.Provider
	// This case is for V5.0 actually as ack messages may return status.
	// To deal with V3.1.1 two ways left:
	//   - ignore the message but send acks
	//   - return error which leads to disconnect
	if status := s.Auth.ACL(s.ID, s.Username, pkt.Topic(), auth.AccessTypeWrite); status == auth.StatusDeny {
		reason = packet.CodeAdministrativeAction
	}

	switch pkt.QoS() {
	case packet.QoS2:
		resp, _ = packet.New(s.Version, packet.PUBREC)
		r, _ := resp.(*packet.Ack)
		id, _ := pkt.ID()

		r.SetPacketID(id)
		r.SetReason(reason)

		// [MQTT-4.3.3-9]
		// store incoming QoS 2 message before sending PUBREC as theoretically PUBREL
		// might come before store in case message store done after write PUBREC
		if reason < packet.CodeUnspecifiedError {
			s.pubIn.store(pkt)
		}
	case packet.QoS1:
		resp, _ = packet.New(s.Version, packet.PUBACK)
		r, _ := resp.(*packet.Ack)

		id, _ := pkt.ID()

		r.SetPacketID(id)
		r.SetReason(reason)
		fallthrough
	case packet.QoS0: // QoS 0
		// [MQTT-4.3.1]
		// [MQTT-4.3.2-4]
		if reason < packet.CodeUnspecifiedError {
			if err = s.publishToTopic(pkt); err != nil {
				s.log.Error("Couldn't publish message",
					zap.String("ClientID", s.ID),
					zap.Uint8("QoS", uint8(pkt.QoS())),
					zap.Error(err))
			}
		}
	}

	return resp, err
}

// onAck handle ack acknowledgment received from remote
func (s *Type) onAck(msg packet.Provider) packet.Provider {
	var resp packet.Provider
	switch mIn := msg.(type) {
	case *packet.Ack:
		switch msg.Type() {
		case packet.PUBACK:
			// remote acknowledged PUBLISH QoS 1 message sent by this server
			s.pubOut.release(msg)
		case packet.PUBREC:
			// remote received PUBLISH message sent by this server
			s.pubOut.release(msg)

			discard := false

			id, _ := msg.ID()

			if s.Version == packet.ProtocolV50 && mIn.Reason() >= packet.CodeUnspecifiedError {
				// v5.9 [MQTT-4.9]
				if s.flowRelease(id) {
					s.signalQuota()
				}

				discard = true
			}

			if !discard {
				resp, _ = packet.New(s.Version, packet.PUBREL)
				r, _ := resp.(*packet.Ack)

				r.SetPacketID(id)

				// 2. Put PUBREL into ack queue
				// Do it before writing into network as theoretically response may come
				// faster than put into queue
				s.pubOut.store(resp)
			}
		case packet.PUBREL:
			// Remote has released PUBLISH
			resp, _ = packet.New(s.Version, packet.PUBCOMP)
			r, _ := resp.(*packet.Ack)

			id, _ := msg.ID()
			r.SetPacketID(id)

			s.pubIn.release(msg)
		case packet.PUBCOMP:
			// PUBREL message has been acknowledged, release from queue
			s.pubOut.release(msg)
		default:
			s.log.Error("Unsupported ack message type",
				zap.String("ClientID", s.ID),
				zap.String("type", msg.Type().Name()))
		}
	default:
		//return err
	}

	return resp
}

func (s *Type) onSubscribe(msg *packet.Subscribe) packet.Provider {
	m, _ := packet.New(s.Version, packet.SUBACK)
	resp, _ := m.(*packet.SubAck)

	id, _ := msg.ID()
	resp.SetPacketID(id)

	var retCodes []packet.ReasonCode
	var retainedPublishes []*packet.Publish

	msg.RangeTopics(func(t string, ops packet.SubscriptionOptions) {
		reason := packet.CodeSuccess // nolint: ineffassign
		//authorized := true
		// TODO: check permissions here

		//if authorized {
		subsID := uint32(0)

		// V5.0 [MQTT-3.8.2.1.2]
		if prop := msg.PropertyGet(packet.PropertySubscriptionIdentifier); prop != nil {
			if v, e := prop.AsInt(); e == nil {
				subsID = v
			}
		}

		subsParams := topicsTypes.SubscriptionParams{
			ID:  subsID,
			Ops: ops,
		}

		if grantedQoS, retained, err := s.Subscriber.Subscribe(t, &subsParams); err != nil {
			// [MQTT-3.9.3]
			if s.Version == packet.ProtocolV50 {
				reason = packet.CodeUnspecifiedError
			} else {
				reason = packet.QosFailure
			}
		} else {
			reason = packet.ReasonCode(grantedQoS)
			retainedPublishes = append(retainedPublishes, retained...)
		}

		retCodes = append(retCodes, reason)
	})

	if err := resp.AddReturnCodes(retCodes); err != nil {
		return nil
	}

	// if the session has been persisted before, then all subscribed messages, including the retained,
	// should also have been put into its persisted message packets which will be loaded to the queues
	// when loadSession.
	if s.WasPersisted {
		s.log.Debug("Persisted session state loaded, skip re-push retained messages which will be pushed from the persisted packets:", zap.String("ClientID", s.ID))
		return resp
	}
	// Now put retained messages into publish queue
	for _, rp := range retainedPublishes {
		if s.SubscribedMessageCompleted(rp){
			s.log.Debug("Skip completed retained message:", zap.String("ClientID", s.ID), zap.Int64("MessageID", rp.GetCreateTimestamp()))
			continue
		}
		if pkt, err := rp.Clone(s.Version); err == nil {
			pkt.SetRetain(true)
			s.onSubscribedPublish(pkt)
			s.log.Debug("Pushed retained message:", zap.String("ClientID", s.ID), zap.Int64("MessageID", rp.GetCreateTimestamp()))
		} else {
			s.log.Error("Couldn't clone PUBLISH message", zap.String("ClientID", s.ID), zap.Error(err))
		}
	}

	return resp
}

func (s *Type) onUnSubscribe(msg *packet.UnSubscribe) packet.Provider {
	var retCodes []packet.ReasonCode

	for _, t := range msg.Topics() {
		// TODO: check permissions here
		authorized := true
		reason := packet.CodeSuccess

		if authorized {
			if err := s.Subscriber.UnSubscribe(t); err != nil {
				s.log.Error("Couldn't unsubscribe from topic", zap.Error(err))
			} else {
				reason = packet.CodeNoSubscriptionExisted
			}
		} else {
			reason = packet.CodeNotAuthorized
		}

		retCodes = append(retCodes, reason)
	}

	m, _ := packet.New(s.Version, packet.UNSUBACK)
	resp, _ := m.(*packet.UnSubAck)

	id, _ := msg.ID()
	resp.SetPacketID(id)
	resp.AddReturnCodes(retCodes) // nolint: errcheck

	return resp
}
