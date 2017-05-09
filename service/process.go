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
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/session"
)

var (
	errDisconnect = errors.New("Disconnect")
)

// processor reads messages from the incoming buffer and processes them
func (s *Type) processor() {
	defer func() {
		// Let's recover from panic if happened
		//if r := recover(); r != nil {
		//	appLog.Errorf("(%s) Recovering from panic: %v", s.CID(), r)
		//}

		s.wgStopped.Done()
		//s.Stop()
		if s.Stop() {
			s.OnClose(s.ID)
		}
	}()

	appLog.Debugf("(%s) Starting processor", s.CID())

	s.wgStarted.Done()

	for {
		// 1. Find out what message is next and the size of the message
		mType, total, err := s.peekMessageSize()
		if err != nil {
			if err != io.EOF {
				appLog.Errorf("(%s) Error peeking next message size: %v", s.CID(), err)
			}
			return
		}

		msg, n, err := s.peekMessage(mType, total)
		if err != nil {
			if err != io.EOF {
				appLog.Errorf("(%s) Error peeking next message: %v", s.CID(), err)
			}
			return
		}

		appLog.Tracef("(%s) Received: %s", s.CID(), msg)

		s.inStat.increment(int64(n))

		// 5. Process the read message
		err = s.processIncoming(msg)
		if err != nil {
			//if err != errDisconnect {
			//	appLog.Errorf("(%s) Error processing %s: %v", s.CID(), msg.Name(), err)
			//} else {
			return
			//}
		}

		// 7. We should commit the bytes in the buffer so we can move on
		_, err = s.in.ReadCommit(total)
		if err != nil {
			if err != io.EOF {
				appLog.Errorf("(%s) Error committing %d read bytes: %v", s.CID(), total, err)
			}
			return
		}

		// 7. Check to see if done is closed, if so, exit
		if s.IsDone() && s.in.Len() == 0 {
			return
		}
	}
}

func (s *Type) processIncoming(msg message.Provider) error {
	var err error

	switch msg := msg.(type) {
	case *message.PublishMessage:
		// For PUBLISH message, we should figure out what QoS it is and process accordingly
		// If QoS == 0, we should just take the next step, no ack required
		// If QoS == 1, we should send back PUBACK, then take the next step
		// If QoS == 2, we need to put it in the ack queue, send back PUBREC
		err = s.processPublish(msg)
	case *message.PubAckMessage:
		// For PUBACK message, it means QoS 1, we should send to ack queue
		s.sess.Pub1ack.Ack(msg) // nolint: errcheck
		s.processAcked(s.sess.Pub1ack)
	case *message.PubRecMessage:
		// For PUBREC message, it means QoS 2, we should send to ack queue, and send back PUBREL
		if err = s.sess.Pub2out.Ack(msg); err != nil {
			break
		}

		resp := message.NewPubRelMessage()
		resp.SetPacketID(msg.PacketID())
		_, err = s.writeMessage(resp)
	case *message.PubRelMessage:
		// For PUBREL message, it means QoS 2, we should send to ack queue, and send back PUBCOMP
		if err = s.sess.Pub2in.Ack(msg); err != nil {
			break
		}

		s.processAcked(s.sess.Pub2in)

		resp := message.NewPubCompMessage()
		resp.SetPacketID(msg.PacketID())
		_, err = s.writeMessage(resp)
	case *message.PubCompMessage:
		// For PUBCOMP message, it means QoS 2, we should send to ack queue
		if err = s.sess.Pub2out.Ack(msg); err != nil {
			break
		}

		s.processAcked(s.sess.Pub2out)
	case *message.SubscribeMessage:
		// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
		return s.processSubscribe(msg)
	case *message.SubAckMessage:
		// For SUBACK message, we should send to ack queue
		s.sess.SubAck.Ack(msg) // nolint: errcheck
		s.processAcked(s.sess.SubAck)
	case *message.UnSubscribeMessage:
		// For UNSUBSCRIBE message, we should remove subscriber, then send back UNSUBACK
		return s.processUnSubscribe(msg)
	case *message.UnSubAckMessage:
		// For UNSUBACK message, we should send to ack queue
		s.sess.UnSubAck.Ack(msg) // nolint: errcheck
		s.processAcked(s.sess.UnSubAck)
	case *message.PingReqMessage:
		// For PINGREQ message, we should send back PINGRESP
		resp := message.NewPingRespMessage()
		_, err = s.writeMessage(resp)
	case *message.PingRespMessage:
		s.sess.PingAck.Ack(msg) // nolint: errcheck
		s.processAcked(s.sess.PingAck)
	case *message.DisconnectMessage:
		// For DISCONNECT message, we should quit without sending Will
		s.disconnectByServer = false
		s.sess.Will = nil
		return errDisconnect
	default:
		return fmt.Errorf("(%s) invalid message type %s", s.CID(), msg.Name())
	}

	if err != nil {
		appLog.Debugf("(%s) Error processing acked message: %v", s.CID(), err)
	}

	return err
}

func (s *Type) processAcked(ackq *session.AckQueue) {
	for _, ackmsg := range ackq.GetAckMsg() {
		// Let's get the messages from the saved message byte slices.
		msg, err := ackmsg.MType.New()
		if err != nil {
			appLog.Errorf("process/processAcked: Unable to creating new %s message: %v", ackmsg.MType, err)
			continue
		}

		if _, err = msg.Decode(ackmsg.MsgBuf); err != nil {
			appLog.Errorf("process/processAcked: Unable to decode %s message: %v", ackmsg.MType, err)
			continue
		}

		var ack message.Provider
		if ack, err = ackmsg.State.New(); err != nil {
			appLog.Errorf("process/processAcked: Unable to creating new %s message: %v", ackmsg.State, err)
			continue
		}

		if _, err = ack.Decode(ackmsg.AckBuf); err != nil {
			appLog.Errorf("process/processAcked: Unable to decode %s message: %v", ackmsg.State, err)
			continue
		}

		//appLog.Debugf("(%s) Processing acked message: %v", this.cid(), ack)

		// - PUBACK if it's QoS 1 message. This is on the client side.
		// - PUBREL if it's QoS 2 message. This is on the server side.
		// - PUBCOMP if it's QoS 2 message. This is on the client side.
		// - SUBACK if it's a subscribe message. This is on the client side.
		// - UNSUBACK if it's a unsubscribe message. This is on the client side.
		switch ackmsg.State {
		case message.PUBREL:
			// If ack is PUBREL, that means the QoS 2 message sent by a remote client is
			// releassed, so let's publish it to other subscribers.
			if err = s.onPublish(msg.(*message.PublishMessage)); err != nil {
				appLog.Errorf("(%s) Error processing ack'ed %s message: %v", s.CID(), ackmsg.MType, err)
			}
		case message.PUBACK, message.PUBCOMP, message.SUBACK, message.UNSUBACK, message.PINGRESP:
			appLog.Debugf("process/processAcked: %s", ack)
			// If ack is PUBACK, that means the QoS 1 message sent by this service got
			// ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is PUBCOMP, that means the QoS 2 message sent by this service got
			// ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is SUBACK, that means the SUBSCRIBE message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is UNSUBACK, that means the SUBSCRIBE message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is PINGRESP, that means the PINGREQ message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			//err = nil
		default:
			appLog.Errorf("(%s) Invalid ack message type %s.", s.CID(), ackmsg.State)
			continue
		}

		// Call the registered onComplete function
		if ackmsg.OnComplete != nil {
			onComplete, ok := ackmsg.OnComplete.(OnCompleteFunc)
			if !ok {
				appLog.Errorf("process/processAcked: Error type asserting onComplete function: %v", reflect.TypeOf(ackmsg.OnComplete))
			} else if onComplete != nil {
				if err := onComplete(msg, ack, nil); err != nil {
					appLog.Errorf("process/processAcked: Error running onComplete(): %v", err)
				}
			}
		}
	}
}

// For PUBLISH message, we should figure out what QoS it is and process accordingly
// If QoS == 0, we should just take the next step, no ack required
// If QoS == 1, we should send back PUBACK, then take the next step
// If QoS == 2, we need to put it in the ack queue, send back PUBREC
func (s *Type) processPublish(msg *message.PublishMessage) error {
	// check for topic access

	switch msg.QoS() {
	case message.QosExactlyOnce:
		s.sess.Pub2in.Wait(msg, nil) // nolint: errcheck

		resp := message.NewPubRecMessage()
		resp.SetPacketID(msg.PacketID())

		_, err := s.writeMessage(resp)
		return err

	case message.QosAtLeastOnce:
		resp := message.NewPubAckMessage()
		resp.SetPacketID(msg.PacketID())

		if _, err := s.writeMessage(resp); err != nil {
			return err
		}

		return s.onPublish(msg)

	case message.QosAtMostOnce:
		return s.onPublish(msg)
	}

	return fmt.Errorf("(%s) invalid message QoS %d", s.CID(), msg.QoS())
}

// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
func (s *Type) processSubscribe(msg *message.SubscribeMessage) error {
	resp := message.NewSubAckMessage()
	resp.SetPacketID(msg.PacketID())

	// Subscribe to the different topics
	var retCodes []message.QosType

	topics := msg.Topics()
	qos := msg.Qos()

	s.rmSgs = s.rmSgs[0:0]

	for i, t := range topics {
		rqos, err := s.TopicsMgr.Subscribe(t, qos[i], &s.onPub)
		if err != nil {
			return err
		}
		s.sess.AddTopic(t, qos[i]) // nolint: errcheck

		retCodes = append(retCodes, rqos)

		// yeah I am not checking errors here. If there's an error we don't want the
		// subscription to stop, just let it go.
		s.TopicsMgr.Retained(t, &s.rmSgs) // nolint: errcheck
		appLog.Debugf("(%s) topic = %s, retained count = %d", s.CID(), t, len(s.rmSgs))
	}

	if err := resp.AddReturnCodes(retCodes); err != nil {
		return err
	}

	if _, err := s.writeMessage(resp); err != nil {
		return err
	}

	for _, rm := range s.rmSgs {
		if err := s.Publish(rm, nil); err != nil {
			appLog.Errorf("service/processSubscribe: Error publishing retained message: %v", err)
			return err
		}
	}

	return nil
}

// For UNSUBSCRIBE message, we should remove the subscriber, and send back UNSUBACK
func (s *Type) processUnSubscribe(msg *message.UnSubscribeMessage) error {
	topics := msg.Topics()

	for _, t := range topics {
		s.TopicsMgr.UnSubscribe(t, &s.onPub) // nolint: errcheck
		s.sess.RemoveTopic(t)                // nolint: errcheck
	}

	resp := message.NewUnSubAckMessage()
	resp.SetPacketID(msg.PacketID())

	_, err := s.writeMessage(resp)
	return err
}

// onPublish() is called when the server receives a PUBLISH message AND have completed
// the ack cycle. This method will get the list of subscribers based on the publish
// topic, and publishes the message to the list of subscribers.
func (s *Type) onPublish(msg *message.PublishMessage) error {
	if msg.Retain() {
		if err := s.TopicsMgr.Retain(msg); err != nil {
			appLog.Errorf("(%s) Error retaining message: %v", s.CID(), err)
		}
	}

	var subscribers []interface{}
	var qoss []message.QosType

	err := s.TopicsMgr.Subscribers(msg.Topic(), msg.QoS(), &subscribers, &qoss)
	if err != nil {
		appLog.Errorf("(%s) Error retrieving subscribers list: %v", s.CID(), err)
		return err
	}

	msg.SetRetain(false)

	appLog.Debugf("(%s) Publishing to topic %q and %d subscribers", s.CID(), msg.Topic(), len(subscribers))
	for _, s := range subscribers {
		if s != nil {

			if onPub, ok := s.(*OnPublishFunc); !ok {
				appLog.Errorf("Invalid onPublish Function")
				continue
			} else {
				(*onPub)(msg) // nolint: errcheck
			}
		}
	}

	return nil
}
