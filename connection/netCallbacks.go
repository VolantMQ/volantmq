package connection

import (
	"container/list"
	"sync/atomic"

	"github.com/troian/surgemq/auth"
	"github.com/troian/surgemq/packet"
	"github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/subscriber"
	"go.uber.org/zap"
)

func (s *Type) getState() *persistenceTypes.SessionMessages {
	encodeMessage := func(m packet.Provider) ([]byte, error) {
		sz, err := m.Size()
		if err != nil {
			return nil, err
		}

		buf := make([]byte, sz)
		if _, err = m.Encode(buf); err != nil {
			return nil, err
		}

		return buf, nil
	}

	outMessages := [][]byte{}
	unAckMessages := [][]byte{}

	var next *list.Element
	for elem := s.tx.messages.Front(); elem != nil; elem = next {
		next = elem.Next()
		switch m := s.tx.messages.Remove(elem).(type) {
		case *packet.Publish:
			qos := m.QoS()
			if qos != packet.QoS0 || (s.offlineQoS0 && qos == packet.QoS0) {
				// make sure message has some IDType to prevent encode error
				m.SetPacketID(0)
				if buf, err := encodeMessage(m); err != nil {
					s.log.Error("Couldn't encode message for persistence", zap.Error(err))
				} else {
					outMessages = append(outMessages, buf)
				}
			}
		case *unacknowledgedPublish:
			if buf, err := encodeMessage(m.msg); err != nil {
				s.log.Error("Couldn't encode message for persistence", zap.Error(err))
			} else {
				unAckMessages = append(unAckMessages, buf)
			}
		}
	}

	for _, m := range s.pubOut.get() {
		switch msg := m.(type) {
		case *packet.Publish:
			if msg.QoS() == packet.QoS1 {
				msg.SetDup(true)
			}
		}

		if buf, err := encodeMessage(m); err != nil {
			s.log.Error("Couldn't encode message for persistence", zap.Error(err))
		} else {
			unAckMessages = append(unAckMessages, buf)
		}
	}

	return &persistenceTypes.SessionMessages{
		OutMessages:   outMessages,
		UnAckMessages: unAckMessages,
	}
}

func (s *Type) onConnectionClose(will bool) {
	s.onConnDisconnect.Do(func() {
		params := &DisconnectParams{
			Will:     will,
			ExpireAt: nil,
		}

		//s.started.Wait()

		if err := s.conn.Close(); err != nil {
			s.log.Error("close connection", zap.String("ClientID", s.id), zap.Error(err))
		}

		close(s.quit)

		s.subscriber.Offline(s.clean)

		s.tx.shutdown()
		s.rx.shutdown()

		if !s.clean {
			params.State = s.getState()
		}

		// [MQTT-3.3.1-7]
		// Discard retained messages with QoS 0
		s.retained.lock.Lock()
		//for _, m := range s.retained.list {
		//	s.topics.Retain(m) // nolint: errcheck
		//}
		s.retained.list = []*packet.Publish{}
		s.retained.lock.Unlock()
		s.conn = nil

		s.onDisconnect(params)
	})
}

// onPublish invoked when server receives PUBLISH message from remote
// On QoS == 0, we should just take the next step, no ack required
// On QoS == 1, send back PUBACK, then take the next step
// On QoS == 2, we need to put it in the ack queue, send back PUBREC
func (s *Type) onPublish(msg *packet.Publish) packet.Provider {
	// check for topic access

	reason := packet.CodeSuccess

	var resp packet.Provider
	// This case is for V5.0 actually as ack messages may return status.
	// To deal with V3.1.1 two ways left:
	//   - ignore the message but send acks
	//   - return error which leads to disconnect
	if status := s.auth.ACL(s.id, s.username, msg.Topic(), auth.AccessTypeWrite); status == auth.StatusDeny {
		reason = packet.CodeAdministrativeAction
	}

	switch msg.QoS() {
	case packet.QoS2:
		resp, _ = packet.NewMessage(s.version, packet.PUBREC)
		r, _ := resp.(*packet.Ack)
		id, _ := msg.ID()

		r.SetPacketID(id)
		r.SetReason(reason)

		// [MQTT-4.3.3-9]
		// store incoming QoS 2 message before sending PUBREC as theoretically PUBREL
		// might come before store in case message store done after write PUBREC
		if reason < packet.CodeUnspecifiedError {
			s.pubIn.store(msg)
		}
	case packet.QoS1:
		resp, _ = packet.NewMessage(s.version, packet.PUBACK)
		r, _ := resp.(*packet.Ack)

		id, _ := msg.ID()
		reason := packet.CodeSuccess

		r.SetPacketID(id)
		r.SetReason(reason)
		//_, err = s.conn.WriteMessage(resp, false)

		// [MQTT-4.3.2-4]
		if reason < packet.CodeUnspecifiedError {
			if err := s.publishToTopic(msg); err != nil {
				s.log.Error("Couldn't publish message",
					zap.String("ClientID", s.id),
					zap.Uint8("QoS", uint8(msg.QoS())),
					zap.Error(err))
			}
		}
	case packet.QoS0: // QoS 0
		// [MQTT-4.3.1]
		if err := s.publishToTopic(msg); err != nil {
			s.log.Error("Couldn't publish message",
				zap.String("ClientID", s.id),
				zap.Uint8("QoS", uint8(msg.QoS())),
				zap.Error(err))
		}
	}

	return resp
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

			if s.version == packet.ProtocolV50 && mIn.Reason() >= packet.CodeUnspecifiedError {
				// v5.9 [MQTT-4.9]
				atomic.AddInt32(&s.sendQuota, 1)

				discard = true
			}

			if !discard {
				resp, _ = packet.NewMessage(s.version, packet.PUBREL)
				r, _ := resp.(*packet.Ack)

				id, _ := msg.ID()
				r.SetPacketID(id)

				// 2. Put PUBREL into ack queue
				// Do it before writing into network as theoretically response may come
				// faster than put into queue
				s.pubOut.store(resp)
			}
		case packet.PUBREL:
			// Remote has released PUBLISH
			resp, _ = packet.NewMessage(s.version, packet.PUBCOMP)
			r, _ := resp.(*packet.Ack)

			id, _ := msg.ID()
			r.SetPacketID(id)

			s.pubIn.release(msg)
		case packet.PUBCOMP:
			// PUBREL message has been acknowledged, release from queue
			s.pubOut.release(msg)
		default:
			s.log.Error("Unsupported ack message type",
				zap.String("ClientID", s.id),
				zap.String("type", msg.Type().Name()))
		}
	default:
		//return err
	}

	return resp
}

func (s *Type) onSubscribe(msg *packet.Subscribe) packet.Provider {
	m, _ := packet.NewMessage(s.version, packet.SUBACK)
	resp, _ := m.(*packet.SubAck)

	id, _ := msg.ID()
	resp.SetPacketID(id)

	var retCodes []packet.ReasonCode
	var retainedMessages []*packet.Publish

	iter := msg.Topics().Iterator()
	for kv, ok := iter(); ok; kv, ok = iter() {
		t := kv.Key.(string)
		ops := kv.Value.(packet.SubscriptionOptions)

		reason := packet.CodeSuccess // nolint: ineffassign
		//authorized := true
		// TODO: check permissions here

		//if authorized {
		subsID := uint32(0)

		// V5.0 [MQTT-3.8.2.1.2]
		if sID, err := msg.PropertyGet(packet.PropertySubscriptionIdentifier); err == nil {
			subsID = sID.(uint32)
		}

		subsParams := subscriber.SubscriptionParams{
			ID:        subsID,
			Requested: ops,
		}

		if rQoS, retained, err := s.subscriber.Subscribe(t, &subsParams); err != nil {
			// [MQTT-3.9.3]ƒ
			if s.version == packet.ProtocolV50 {
				reason = packet.CodeUnspecifiedError
			} else {
				reason = packet.QosFailure
			}
		} else {
			reason = packet.ReasonCode(rQoS)
			retainedMessages = append(retainedMessages, retained...)
		}

		retCodes = append(retCodes, reason)

		s.log.Debug("Subscribing",
			zap.String("ClientID", s.id),
			zap.String("topic", t),
			zap.Uint8("result_code", uint8(reason)))
	}

	if err := resp.AddReturnCodes(retCodes); err != nil {
		return nil
	}

	// Now put retained messages into publish queue
	for _, rm := range retainedMessages {
		m, _ := packet.NewMessage(s.version, packet.PUBLISH)
		msg, _ := m.(*packet.Publish)

		// [MQTT-3.3.1-8]
		msg.Set(rm.Topic(), rm.Payload(), rm.QoS(), true, false) // nolint: errcheck

		s.tx.sendPacket(msg)
	}

	return resp
}

func (s *Type) onUnSubscribe(msg *packet.UnSubscribe) packet.Provider {
	iter := msg.Topics().Iterator()

	var retCodes []packet.ReasonCode

	for kv, ok := iter(); ok; kv, ok = iter() {
		t := kv.Key.(string)
		// TODO: check permissions here
		authorized := true
		reason := packet.CodeSuccess

		if authorized {
			if err := s.subscriber.UnSubscribe(t); err != nil {
				s.log.Error("Couldn't unsubscribe from topic", zap.Error(err))
			} else {
				reason = packet.CodeNoSubscriptionExisted
			}
		} else {
			reason = packet.CodeNotAuthorized
		}

		retCodes = append(retCodes, reason)
	}

	m, _ := packet.NewMessage(s.version, packet.UNSUBACK)
	resp, _ := m.(*packet.UnSubAck)

	id, _ := msg.ID()
	resp.SetPacketID(id)
	resp.AddReturnCodes(retCodes) // nolint: errcheck

	return resp
}
