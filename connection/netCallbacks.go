package connection

import (
	"sync/atomic"

	"github.com/VolantMQ/volantmq/packet"
	"go.uber.org/zap"
)

func (s *impl) txShutdown() {
	atomic.StoreUint32(&s.txRunning, 2)
	s.txTimer.Stop()
	s.txWg.Wait()

	select {
	case <-s.txAvailable:
	default:
		close(s.txAvailable)
	}
}

func (s *impl) rxShutdown() {
	atomic.StoreUint32(&s.rxRunning, 0)
	s.rxWg.Wait()
}

func (s *impl) onConnectionClose(status error) bool {
	return s.onConnDisconnect.Do(func() {
		s.keepAliveTimer.Stop()
		close(s.quit)

		var err error
		// shutdown quit channel tells all routines finita la commedia
		s.ePoll.Stop(s.desc)

		if s.state != stateConnecting && s.state != stateAuth && s.state != stateConnectFailed {
			s.SignalOffline()
		} else if s.state == stateConnecting || s.state == stateAuth {
			select {
			case <-s.connect:
			default:
				close(s.connect)
			}
		}

		// clean up transmitter to allow send disconnect command to client if needed
		s.txShutdown()

		if reason, ok := status.(packet.ReasonCode); ok &&
			reason != packet.CodeSuccess && s.version >= packet.ProtocolV50 &&
			s.state != stateConnecting && s.state != stateAuth && s.state != stateConnectFailed {
			// server wants to tell client disconnect reason
			pkt := packet.NewDisconnect(s.version)
			pkt.SetReasonCode(reason)

			var buf []byte
			if buf, err = packet.Encode(pkt); err != nil {
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

		if err = s.desc.Close(); err != nil {
			s.log.Error("Close polling descriptor", zap.String("ClientID", s.id), zap.Error(err))
		}

		if err = s.conn.Close(); err != nil {
			s.log.Error("close connection", zap.String("ClientID", s.id), zap.Error(err))
		}

		s.rxShutdown()

		s.conn = nil

		if s.state != stateConnecting && s.state != stateAuth && s.state != stateConnectFailed {
			params := DisconnectParams{
				Packets: s.getToPersist(),
				Reason:  packet.CodeSuccess,
			}

			if rc, ok := err.(packet.ReasonCode); ok {
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
func (s *impl) onPublish(pkt *packet.Publish) (packet.Provider, error) {
	// check for topic access
	var err error
	reason := packet.CodeSuccess

	if s.version >= packet.ProtocolV50 {
		if !s.retainAvailable && pkt.Retain() {
			return nil, packet.CodeRetainNotSupported
		}

		if prop := pkt.PropertyGet(packet.PropertyTopicAlias); prop != nil {
			if val, ok := prop.AsShort(); ok == nil && (val == 0 || val > s.maxRxTopicAlias) {
				return nil, packet.CodeInvalidTopicAlias
			}
		}
	}

	var resp packet.Provider
	// This case is for V5.0 actually as ack messages may return status.
	// To deal with V3.1.1 two ways left:
	//   - ignore the message but send acks
	//   - return error which leads to disconnect
	//if status := s.ACL(s.ID, pkt.Topic(), auth.AccessTypeWrite); status == auth.StatusDeny {
	//	reason = packet.CodeAdministrativeAction
	//}

	switch pkt.QoS() {
	case packet.QoS2:
		if s.rxQuota == 0 {
			reason = packet.CodeReceiveMaximumExceeded
		} else {
			s.rxQuota--
			resp, _ = packet.New(s.version, packet.PUBREC)
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
		}
	case packet.QoS1:
		resp, _ = packet.New(s.version, packet.PUBACK)
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
					zap.String("ClientID", s.id),
					zap.Uint8("QoS", uint8(pkt.QoS())),
					zap.Error(err))
			}
		}
	}

	return resp, err
}

// onAck handle ack acknowledgment received from remote
func (s *impl) onAck(msg packet.Provider) (packet.Provider, error) {
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

			if s.version == packet.ProtocolV50 && mIn.Reason() >= packet.CodeUnspecifiedError {
				// v5.9 [MQTT-4.9]
				if s.flowRelease(id) {
					s.signalQuota()
				}

				discard = true
			}

			if !discard {
				resp, _ = packet.New(s.version, packet.PUBREL)
				r, _ := resp.(*packet.Ack)

				r.SetPacketID(id)

				// 2. Put PUBREL into ack queue
				// Do it before writing into network as theoretically response may come
				// faster than put into queue
				s.pubOut.store(resp)
			}
		case packet.PUBREL:
			// Remote has released PUBLISH
			resp, _ = packet.New(s.version, packet.PUBCOMP)
			r, _ := resp.(*packet.Ack)

			id, _ := msg.ID()
			r.SetPacketID(id)

			s.rxQuota++
			s.pubIn.release(msg)
		case packet.PUBCOMP:
			// PUBREL message has been acknowledged, release from queue
			s.pubOut.release(msg)
		default:
			s.log.Error("Unsupported ack message type",
				zap.String("ClientID", s.id),
				zap.String("type", msg.Type().Name()))
		}
	}

	return resp, nil
}
