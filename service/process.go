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

	"github.com/surge/glog"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/sessions"
)

var (
	errDisconnect = errors.New("Disconnect")
)

// processor() reads messages from the incoming buffer and processes them
func (s *service) processor() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			//glog.Errorf("(%s) Recovering from panic: %v", this.cid(), r)
		}

		s.wgStopped.Done()
		s.stop()

		//glog.Debugf("(%s) Stopping processor", this.cid())
	}()

	glog.Debugf("(%s) Starting processor", s.cid())

	s.wgStarted.Done()

	for {
		// 1. Find out what message is next and the size of the message
		mtype, total, err := s.peekMessageSize()
		if err != nil {
			//if err != io.EOF {
			glog.Errorf("(%s) Error peeking next message size: %v", s.cid(), err)
			//}
			return
		}

		msg, n, err := s.peekMessage(mtype, total)
		if err != nil {
			//if err != io.EOF {
			glog.Errorf("(%s) Error peeking next message: %v", s.cid(), err)
			//}
			return
		}

		//glog.Debugf("(%s) Received: %s", this.cid(), msg)

		s.inStat.increment(int64(n))

		// 5. Process the read message
		err = s.processIncoming(msg)
		if err != nil {
			if err != errDisconnect {
				glog.Errorf("(%s) Error processing %s: %v", s.cid(), msg.Name(), err)
			} else {
				return
			}
		}

		// 7. We should commit the bytes in the buffer so we can move on
		_, err = s.in.ReadCommit(total)
		if err != nil {
			if err != io.EOF {
				glog.Errorf("(%s) Error committing %d read bytes: %v", s.cid(), total, err)
			}
			return
		}

		// 7. Check to see if done is closed, if so, exit
		if s.isDone() && s.in.Len() == 0 {
			return
		}

		//if this.inStat.msgs%1000 == 0 {
		//	glog.Debugf("(%s) Going to process message %d", this.cid(), this.inStat.msgs)
		//}
	}
}

func (s *service) processIncoming(msg message.Message) error {
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
		s.sess.Pub1ack.Ack(msg)
		s.processAcked(s.sess.Pub1ack)

	case *message.PubRecMessage:
		// For PUBREC message, it means QoS 2, we should send to ack queue, and send back PUBREL
		if err = s.sess.Pub2out.Ack(msg); err != nil {
			break
		}

		resp := message.NewPubRelMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = s.writeMessage(resp)

	case *message.PubRelMessage:
		// For PUBREL message, it means QoS 2, we should send to ack queue, and send back PUBCOMP
		if err = s.sess.Pub2in.Ack(msg); err != nil {
			break
		}

		s.processAcked(s.sess.Pub2in)

		resp := message.NewPubCompMessage()
		resp.SetPacketId(msg.PacketId())
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
		s.sess.Suback.Ack(msg)
		s.processAcked(s.sess.Suback)

	case *message.UnSubscribeMessage:
		// For UNSUBSCRIBE message, we should remove subscriber, then send back UNSUBACK
		return s.processUnsubscribe(msg)

	case *message.UnSubAckMessage:
		// For UNSUBACK message, we should send to ack queue
		s.sess.Unsuback.Ack(msg)
		s.processAcked(s.sess.Unsuback)

	case *message.PingReqMessage:
		// For PINGREQ message, we should send back PINGRESP
		resp := message.NewPingRespMessage()
		_, err = s.writeMessage(resp)

	case *message.PingRespMessage:
		s.sess.Pingack.Ack(msg)
		s.processAcked(s.sess.Pingack)

	case *message.DisconnectMessage:
		// For DISCONNECT message, we should quit
		s.sess.Cmsg.SetWillFlag(false)
		return errDisconnect

	default:
		return fmt.Errorf("(%s) invalid message type %s", s.cid(), msg.Name())
	}

	if err != nil {
		glog.Debugf("(%s) Error processing acked message: %v", s.cid(), err)
	}

	return err
}

func (s *service) processAcked(ackq *sessions.Ackqueue) {
	for _, ackmsg := range ackq.Acked() {
		// Let's get the messages from the saved message byte slices.
		msg, err := ackmsg.Mtype.New()
		if err != nil {
			glog.Errorf("process/processAcked: Unable to creating new %s message: %v", ackmsg.Mtype, err)
			continue
		}

		if _, err = msg.Decode(ackmsg.Msgbuf); err != nil {
			glog.Errorf("process/processAcked: Unable to decode %s message: %v", ackmsg.Mtype, err)
			continue
		}

		var ack message.Message
		if ack, err = ackmsg.State.New(); err != nil {
			glog.Errorf("process/processAcked: Unable to creating new %s message: %v", ackmsg.State, err)
			continue
		}

		if _, err = ack.Decode(ackmsg.Ackbuf); err != nil {
			glog.Errorf("process/processAcked: Unable to decode %s message: %v", ackmsg.State, err)
			continue
		}

		//glog.Debugf("(%s) Processing acked message: %v", this.cid(), ack)

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
				glog.Errorf("(%s) Error processing ack'ed %s message: %v", s.cid(), ackmsg.Mtype, err)
			}
		case message.PUBACK, message.PUBCOMP, message.SUBACK, message.UNSUBACK, message.PINGRESP:
			glog.Debugf("process/processAcked: %s", ack)
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
			glog.Errorf("(%s) Invalid ack message type %s.", s.cid(), ackmsg.State)
			continue
		}

		// Call the registered onComplete function
		if ackmsg.OnComplete != nil {
			onComplete, ok := ackmsg.OnComplete.(OnCompleteFunc)
			if !ok {
				glog.Errorf("process/processAcked: Error type asserting onComplete function: %v", reflect.TypeOf(ackmsg.OnComplete))
			} else if onComplete != nil {
				if err := onComplete(msg, ack, nil); err != nil {
					glog.Errorf("process/processAcked: Error running onComplete(): %v", err)
				}
			}
		}
	}
}

// For PUBLISH message, we should figure out what QoS it is and process accordingly
// If QoS == 0, we should just take the next step, no ack required
// If QoS == 1, we should send back PUBACK, then take the next step
// If QoS == 2, we need to put it in the ack queue, send back PUBREC
func (s *service) processPublish(msg *message.PublishMessage) error {
	switch msg.QoS() {
	case message.QosExactlyOnce:
		s.sess.Pub2in.Wait(msg, nil)

		resp := message.NewPubRecMessage()
		resp.SetPacketId(msg.PacketId())

		_, err := s.writeMessage(resp)
		return err

	case message.QosAtLeastOnce:
		resp := message.NewPubAckMessage()
		resp.SetPacketId(msg.PacketId())

		if _, err := s.writeMessage(resp); err != nil {
			return err
		}

		return s.onPublish(msg)

	case message.QosAtMostOnce:
		return s.onPublish(msg)
	}

	return fmt.Errorf("(%s) invalid message QoS %d", s.cid(), msg.QoS())
}

// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
func (s *service) processSubscribe(msg *message.SubscribeMessage) error {
	resp := message.NewSubAckMessage()
	resp.SetPacketId(msg.PacketId())

	// Subscribe to the different topics
	var retcodes []byte

	topics := msg.Topics()
	qos := msg.Qos()

	s.rmsgs = s.rmsgs[0:0]

	for i, t := range topics {
		rqos, err := s.topicsMgr.Subscribe(t, qos[i], &s.onpub)
		if err != nil {
			return err
		}
		s.sess.AddTopic(string(t), qos[i])

		retcodes = append(retcodes, rqos)

		// yeah I am not checking errors here. If there's an error we don't want the
		// subscription to stop, just let it go.
		s.topicsMgr.Retained(t, &s.rmsgs)
		glog.Debugf("(%s) topic = %s, retained count = %d", s.cid(), string(t), len(s.rmsgs))
	}

	if err := resp.AddReturnCodes(retcodes); err != nil {
		return err
	}

	if _, err := s.writeMessage(resp); err != nil {
		return err
	}

	for _, rm := range s.rmsgs {
		if err := s.publish(rm, nil); err != nil {
			glog.Errorf("service/processSubscribe: Error publishing retained message: %v", err)
			return err
		}
	}

	return nil
}

// For UNSUBSCRIBE message, we should remove the subscriber, and send back UNSUBACK
func (s *service) processUnsubscribe(msg *message.UnSubscribeMessage) error {
	topics := msg.Topics()

	for _, t := range topics {
		s.topicsMgr.UnSubscribe(t, &s.onpub)
		s.sess.RemoveTopic(string(t))
	}

	resp := message.NewUnSubAckMessage()
	resp.SetPacketId(msg.PacketId())

	_, err := s.writeMessage(resp)
	return err
}

// onPublish() is called when the server receives a PUBLISH message AND have completed
// the ack cycle. This method will get the list of subscribers based on the publish
// topic, and publishes the message to the list of subscribers.
func (s *service) onPublish(msg *message.PublishMessage) error {
	if msg.Retain() {
		if err := s.topicsMgr.Retain(msg); err != nil {
			glog.Errorf("(%s) Error retaining message: %v", s.cid(), err)
		}
	}

	err := s.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &s.subs, &s.qoss)
	if err != nil {
		glog.Errorf("(%s) Error retrieving subscribers list: %v", s.cid(), err)
		return err
	}

	msg.SetRetain(false)

	//glog.Debugf("(%s) Publishing to topic %q and %d subscribers", this.cid(), string(msg.Topic()), len(this.subs))
	for _, s := range s.subs {
		if s != nil {
			fn, ok := s.(*OnPublishFunc)
			if !ok {
				glog.Errorf("Invalid onPublish Function")
				return errors.New("Invalid onPublish Function")
			}

			if err := (*fn)(msg); err != nil {
				glog.Errorf("Error onPublish")
			}
		}
	}

	return nil
}
