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

	//messages := s.publisher.messages.GetAll()

	//for _, v := range messages {
	var next *list.Element
	for elem := s.publisher.messages.Front(); elem != nil; elem = next {
		next = elem.Next()
		switch m := s.publisher.messages.Remove(elem).(type) {
		case *packet.Publish:
			qos := m.QoS()
			if qos != packet.QoS0 || (s.offlineQoS0 && qos == packet.QoS0) {
				// make sure message has some IDType to prevent encode error
				m.SetPacketID(0)
				if buf, err := encodeMessage(m); err != nil {
					s.log.prod.Error("Couldn't encode message for persistence", zap.Error(err))
				} else {
					outMessages = append(outMessages, buf)
				}
			}
		case *unacknowledgedPublish:
			if buf, err := encodeMessage(m.msg); err != nil {
				s.log.prod.Error("Couldn't encode message for persistence", zap.Error(err))
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
			s.log.prod.Error("Couldn't encode message for persistence", zap.Error(err))
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

		s.started.Wait()

		s.subscriber.Offline(s.clean)

		close(s.quit)
		s.publisher.cond.L.Lock()
		s.publisher.cond.Broadcast()
		s.publisher.cond.L.Unlock()

		// Wait writer to finish it's job
		s.publisher.stopped.Wait()

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
func (s *Type) onPublish(msg *packet.Publish) error {
	// check for topic access
	var err error

	reason := packet.CodeSuccess

	// This case is for V5.0 actually as ack messages may return status.
	// To deal with V3.1.1 two ways left:
	//   - ignore the message but send acks
	//   - return error which leads to disconnect
	if status := s.auth.ACL(s.id, s.username, msg.Topic(), auth.AccessTypeWrite); status == auth.StatusDeny {
		reason = packet.CodeAdministrativeAction
	}

	switch msg.QoS() {
	case packet.QoS2:
		m, _ := packet.NewMessage(s.version, packet.PUBREC)
		resp, _ := m.(*packet.Ack)
		id, _ := msg.ID()

		resp.SetPacketID(id)
		resp.SetReason(reason)

		_, err = s.conn.WriteMessage(resp, false)
		// [MQTT-4.3.3-9]
		// store incoming QoS 2 message before sending PUBREC as theoretically PUBREL
		// might come before store in case message store done after write PUBREC
		if err == nil && reason < packet.CodeUnspecifiedError {
			s.pubIn.store(msg)
		}
	case packet.QoS1:
		m, _ := packet.NewMessage(s.version, packet.PUBACK)
		resp, _ := m.(*packet.Ack)

		id, _ := msg.ID()
		reason := packet.CodeSuccess

		resp.SetPacketID(id)
		resp.SetReason(reason)
		_, err = s.conn.WriteMessage(resp, false)

		// [MQTT-4.3.2-4]
		if err == nil && reason < packet.CodeUnspecifiedError {
			if err = s.publishToTopic(msg); err != nil {
				s.log.prod.Error("Couldn't publish message",
					zap.String("ClientID", s.id),
					zap.Uint8("QoS", uint8(msg.QoS())),
					zap.Error(err))
			}
		}
	case packet.QoS0: // QoS 0
		// [MQTT-4.3.1]
		err = s.publishToTopic(msg)
	}

	return err
}

// onAck handle ack acknowledgment received from remote
func (s *Type) onAck(msg packet.Provider) error {
	var err error

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
				m, _ := packet.NewMessage(s.version, packet.PUBREL)
				resp, _ := m.(*packet.Ack)

				id, _ := msg.ID()
				resp.SetPacketID(id)

				// 2. Put PUBREL into ack queue
				// Do it before writing into network as theoretically response may come
				// faster than put into queue
				s.pubOut.store(resp)

				// 2. Try send PUBREL reply
				_, err = s.conn.WriteMessage(resp, false)
			}
		case packet.PUBREL:
			// Remote has released PUBLISH
			m, _ := packet.NewMessage(s.version, packet.PUBCOMP)
			resp, _ := m.(*packet.Ack)

			id, _ := msg.ID()
			resp.SetPacketID(id)

			s.pubIn.release(msg)

			_, err = s.conn.WriteMessage(resp, false)
		case packet.PUBCOMP:
			// PUBREL message has been acknowledged, release from queue
			s.pubOut.release(msg)
		default:
			s.log.prod.Error("Unsupported ack message type",
				zap.String("ClientID", s.id),
				zap.String("type", msg.Type().Name()))
		}
	default:
		//return err
	}

	return err
}

func (s *Type) onSubscribe(msg *packet.Subscribe) error {
	m, _ := packet.NewMessage(s.version, packet.SUBACK)
	resp, _ := m.(*packet.SubAck)

	id, _ := msg.ID()
	resp.SetPacketID(id)

	var retCodes []packet.ReasonCode
	var retainedMessages []*packet.Publish

	iter := msg.Topics().Iterator()
	for kv, ok := iter(); ok; kv, ok = iter() {
		//for _, t := range topics {
		// Let topic manager know we want to listen to given topic
		//qos := msg.TopicQos(t)
		t := kv.Key.(string)
		ops := kv.Value.(packet.SubscriptionOptions)

		reason := packet.CodeSuccess
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

		s.log.dev.Debug("Subscribing",
			zap.String("ClientID", s.id),
			zap.String("topic", t),
			zap.Uint8("result_code", uint8(reason)))
	}

	if err := resp.AddReturnCodes(retCodes); err != nil {
		return err
	}

	if _, err := s.conn.WriteMessage(resp, false); err != nil {
		s.log.prod.Error("Couldn't send SUBACK. Proceed to unsubscribe",
			zap.String("ClientID", s.id),
			zap.Error(err))

		iter = msg.Topics().Iterator()
		for kv, ok := iter(); ok; kv, ok = iter() {
			t := kv.Key.(string)

			if err = s.subscriber.UnSubscribe(t); err != nil {
				s.log.prod.Error("Couldn't unsubscribe from topic", zap.Error(err))
			}
		}

		return err
	}

	// Now put retained messages into publish queue
	for _, rm := range retainedMessages {
		m, _ := packet.NewMessage(s.version, packet.PUBLISH)
		msg, _ := m.(*packet.Publish)

		// [MQTT-3.3.1-8]
		msg.Set(rm.Topic(), rm.Payload(), rm.QoS(), true, false) // nolint: errcheck

		s.publisher.pushBack(m)
	}

	return nil
}

func (s *Type) onUnSubscribe(msg *packet.UnSubscribe) (*packet.UnSubAck, error) {
	iter := msg.Topics().Iterator()

	var retCodes []packet.ReasonCode

	for kv, ok := iter(); ok; kv, ok = iter() {
		t := kv.Key.(string)
		// TODO: check permissions here
		authorized := true
		reason := packet.CodeSuccess

		if authorized {
			if err := s.subscriber.UnSubscribe(t); err != nil {
				s.log.prod.Error("Couldn't unsubscribe from topic", zap.Error(err))
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

	return resp, nil
}
