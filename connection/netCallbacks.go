package connection

import (
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"go.uber.org/zap"
)

func (s *impl) onConnectionClose(status error) bool {
	return s.onConnDisconnect.Do(func() {
		// shutdown quit channel tells all routines finita la commedia
		close(s.quit)

		var err error
		//s.conn.Stop()
		s.conn.SetReadDeadline(time.Time{})

		if s.state == stateConnecting || s.state == stateAuth {
			select {
			case <-s.connect:
			default:
				close(s.connect)
			}
		}

		// clean up transmitter to allow send disconnect command to client if needed
		s.txShutdown()

		if reason, ok := status.(mqttp.ReasonCode); ok &&
			reason != mqttp.CodeSuccess && s.version >= mqttp.ProtocolV50 &&
			s.state != stateConnecting && s.state != stateAuth && s.state != stateConnectFailed {
			// server wants to tell client disconnect reason
			pkt := mqttp.NewDisconnect(s.version)
			pkt.SetReasonCode(reason)

			var buf []byte
			if buf, err = mqttp.Encode(pkt); err != nil {
				s.log.Error("encode disconnect packet", zap.String("ClientID", s.id), zap.Error(err))
			} else {
				var written int
				if written, err = s.conn.Write(buf); written != len(buf) {
					s.log.Error("Couldn't write disconnect message",
						zap.String("ClientID", s.id),
						zap.Int("packet size", len(buf)),
						zap.Int("written", written))
				} else if err != nil {
					s.log.Debug("Couldn't write disconnect message",
						zap.String("ClientID", s.id),
						zap.Error(err))
				}
			}
		}

		if err = s.conn.Close(); err != nil {
			s.log.Error("close connection", zap.String("ClientID", s.id), zap.Error(err))
		}
		s.rxShutdown()

		s.conn = nil

		if s.state != stateConnecting && s.state != stateAuth && s.state != stateConnectFailed {
			s.SignalOffline()
			params := DisconnectParams{
				Packets: s.getQueuedPackets(),
				Reason:  mqttp.CodeSuccess,
			}

			if rc, ok := err.(mqttp.ReasonCode); ok {
				params.Reason = rc
			}

			s.SignalConnectionClose(params)
		}

		s.state = stateDisconnected
	})
}

// onPublish invoked when server receives PUBLISH message from remote
// On QoS == 0, we should just take the next step, no ack required
// On QoS == 1, send back PUBACK, then take the next step
// On QoS == 2, we need to put it in the ack queue, send back PUBREC
func (s *impl) onPublish(pkt *mqttp.Publish) (mqttp.Provider, error) {
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

	var resp mqttp.Provider
	// This case is for V5.0 actually as ack messages may return status.
	// To deal with V3.1.1 two ways left:
	//   - ignore the message but send acks
	//   - return error which leads to disconnect
	//if status := s.ACL(s.ID, pkt.Topic(), auth.AccessTypeWrite); status == auth.StatusDeny {
	//	reason = mqttp.CodeAdministrativeAction
	//}

	switch pkt.QoS() {
	case mqttp.QoS2:
		if s.rxQuota == 0 {
			reason = mqttp.CodeReceiveMaximumExceeded
		} else {
			s.rxQuota--
			r := mqttp.NewPubRec(s.version)
			id, _ := pkt.ID()

			r.SetPacketID(id)

			resp = r

			// if reason < mqttp.CodeUnspecifiedError {
			// [MQTT-4.3.3-9]
			// store incoming QoS 2 message before sending PUBREC as theoretically PUBREL
			// might come before store in case message store done after write PUBREC
			if reason < mqttp.CodeUnspecifiedError {
				s.pubIn.store(pkt)
			}
			//if !s.pubIn.store(pkt) {
			//	reason = mqttp.CodeReceiveMaximumExceeded
			//}
			// }

			r.SetReason(mqttp.CodeSuccess)
		}
	case mqttp.QoS1:
		r := mqttp.NewPubAck(s.version)

		id, _ := pkt.ID()

		r.SetPacketID(id)
		r.SetReason(reason)
		resp = r
		fallthrough
	case mqttp.QoS0: // QoS 0
		// [MQTT-4.3.1]
		// [MQTT-4.3.2-4]
		// if reason < mqttp.CodeUnspecifiedError {
		if err = s.publishToTopic(pkt); err != nil {
			s.log.Error("Couldn't publish message",
				zap.String("ClientID", s.id),
				zap.Uint8("QoS", uint8(pkt.QoS())),
				zap.Error(err))
		}
		// }
	}

	return resp, err
}

// onAck handle ack acknowledgment received from remote
func (s *impl) onAck(pkt mqttp.Provider) (mqttp.Provider, error) {
	var resp mqttp.Provider
	switch mIn := pkt.(type) {
	case *mqttp.Ack:
		switch pkt.Type() {
		case mqttp.PUBACK:
			// remote acknowledged PUBLISH QoS 1 message sent by this server
			s.pubOut.release(pkt)
		case mqttp.PUBREC:
			// remote received PUBLISH message sent by this server
			s.pubOut.release(pkt)

			discard := false

			id, _ := pkt.ID()

			if s.version == mqttp.ProtocolV50 && mIn.Reason() >= mqttp.CodeUnspecifiedError {
				// v5.0 [MQTT-4.9]
				if s.flowRelease(id) {
					s.signalQuota()
				}

				discard = true
			}

			if !discard {
				resp, _ = mqttp.New(s.version, mqttp.PUBREL)
				r, _ := resp.(*mqttp.Ack)

				r.SetPacketID(id)

				// 2. Put PUBREL into ack queue
				// Do it before writing into network as theoretically response may come
				// faster than put into queue
				s.pubOut.store(resp)
			}
		case mqttp.PUBREL:
			// Remote has released PUBLISH
			resp, _ = mqttp.New(s.version, mqttp.PUBCOMP)
			r, _ := resp.(*mqttp.Ack)

			id, _ := pkt.ID()
			r.SetPacketID(id)

			s.rxQuota++
			s.pubIn.release(pkt)
		case mqttp.PUBCOMP:
			// PUBREL message has been acknowledged, release from queue
			s.pubOut.release(pkt)
		default:
			s.log.Error("Unsupported ack message type",
				zap.String("ClientID", s.id),
				zap.String("type", pkt.Type().Name()))
		}
	}

	return resp, nil
}
