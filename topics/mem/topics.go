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

package mem

import (
	"reflect"
	"sync"

	"errors"

	"github.com/troian/surgemq"
	"github.com/troian/surgemq/message"
	persistenceTypes "github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics"
	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
)

//var (
//	// MaxQosAllowed is the maximum QOS supported by this server
//	MaxQosAllowed = message.QosExactlyOnce
//)

type provider struct {
	// Sub/unSub mutex
	smu sync.RWMutex

	// Subscription tree
	sRoot *sNode

	// Retained message mutex
	rmu sync.RWMutex

	// Retained messages topic tree
	rRoot *rNode

	stat systree.TopicsStat

	persist persistenceTypes.Retained

	log struct {
		prod *zap.Logger
		dev  *zap.Logger
	}
}

var _ topics.Provider = (*provider)(nil)

func init() {
	topics.Register("mem", NewMemProvider())
}

// NewMemProvider returns an new instance of the provider, which is implements the
// TopicsProvider interface. provider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persistend so
// when the server goes, everything will be gone. Use with care.
func NewMemProvider() topics.Provider {
	p := &provider{
		sRoot: newSNode(),
		rRoot: newRNode(),
	}

	p.log.prod = surgemq.GetProdLogger().Named("topics.mem")
	p.log.dev = surgemq.GetDevLogger().Named("topics.mem")

	return p
}

func (mT *provider) Configure(stat systree.TopicsStat, persist persistenceTypes.Retained) error {
	mT.stat = stat
	mT.persist = persist

	entries, err := persist.Load()
	if err != nil && err != persistenceTypes.ErrNotFound {
		return err
	}

	for _, msg := range entries {
		// Loading retained messages
		if m, ok := msg.(*message.PublishMessage); ok {
			mT.log.dev.Debug("Loading retained message", zap.String("topic", m.Topic()), zap.Int8("QoS", int8(m.QoS())))
			mT.Retain(m) // nolint: errcheck
		}
	}

	persist.Delete() // nolint: errcheck

	return nil
}

func (mT *provider) Subscribe(topic string, qos message.QosType, sub *types.Subscriber) (message.QosType, error) {
	if !qos.IsValid() {
		return message.QosFailure, message.ErrInvalidQoS
	}

	if sub == nil {
		return message.QosFailure, errors.New("Subscriber cannot be nil")
	}

	mT.smu.Lock()
	defer mT.smu.Unlock()

	//if qos > MaxQosAllowed {
	//	qos = MaxQosAllowed
	//}

	if err := mT.sRoot.insert(topic, qos, sub); err != nil {
		return message.QosFailure, err
	}

	return qos, nil
}

func (mT *provider) UnSubscribe(topic string, sub *types.Subscriber) error {
	mT.smu.Lock()
	defer mT.smu.Unlock()

	return mT.sRoot.remove(topic, sub)
}

func (mT *provider) Publish(msg *message.PublishMessage) error {
	mT.smu.RLock()

	var subs types.Subscribers

	if err := mT.sRoot.match(msg.Topic(), msg.QoS(), &subs); err != nil {
		mT.smu.RUnlock()
		return err
	}
	mT.smu.RUnlock()

	for _, s := range subs {
		if s != nil {
			if err := s.Publish(msg); err != nil {
				mT.log.prod.Error("Error", zap.Error(err))
			}

			s.WgWriters.Done()
		}
	}

	return nil
}

func (mT *provider) Retain(msg *message.PublishMessage) error {
	mT.rmu.Lock()
	defer mT.rmu.Unlock()

	// [MQTT-3.3.1-10]            [MQTT-3.3.1-7]
	if len(msg.Payload()) == 0 || msg.QoS() == message.QosAtMostOnce {
		mT.rRoot.remove(msg.Topic()) // nolint: errcheck, gas

		if len(msg.Payload()) == 0 {
			return nil
		}
	}

	return mT.rRoot.insert(msg.Topic(), msg)
}

func (mT *provider) Retained(topic string, msgs *[]*message.PublishMessage) error {
	mT.rmu.RLock()
	defer mT.rmu.RUnlock()

	// [MQTT-3.3.1-5]
	return mT.rRoot.match(topic, msgs)
}

func (mT *provider) Close() error {
	var rMsg []*message.PublishMessage
	mT.Retained("#", &rMsg) // nolint: errcheck

	toStore := []message.Provider{}

	for _, m := range rMsg {
		toStore = append(toStore, m)
	}

	if len(toStore) > 0 {
		mT.log.dev.Debug("Storing retained messages", zap.Int("amount", len(toStore)))
		mT.persist.Store(toStore) // nolint: errcheck
	}

	mT.sRoot = nil
	mT.rRoot = nil
	return nil
}

// nolint
const (
	stateCHR byte = iota // Regular character
	stateMWC             // Multi-level wildcard
	stateSWC             // Single-level wildcard
	stateSEP             // Topic level separator
	stateSYS             // System level topic ($)
)

// Returns topic level, remaining topic levels and any errors
func nextTopicLevel(topic string) (string, string, error) {
	s := stateCHR

	for i, c := range topic {
		switch c {
		case '/':
			if s == stateMWC {
				return "", "", errors.New("memtopics/nextTopicLevel: Multi-level wildcard found in topic and it's not at the last level")
			}

			if i == 0 {
				return topics.SWC, topic[i+1:], nil
			}

			return topic[:i], topic[i+1:], nil

		case '#':
			if i != 0 {
				return "", "", errors.New("memtopics/nextTopicLevel: Wildcard character '#' must occupy entire topic level")
			}

			s = stateMWC

		case '+':
			if i != 0 {
				return "", "", errors.New("memtopics/nextTopicLevel: Wildcard character '+' must occupy entire topic level")
			}

			s = stateSWC

		case '$':
			if i == 0 {
				return "", "", errors.New("memtopics/nextTopicLevel: Cannot publish to $ topics")
			}

			s = stateSYS

		default:
			if s == stateMWC || s == stateSWC {
				return "", "", errors.New("memtopics/nextTopicLevel: Wildcard characters '#' and '+' must occupy entire topic level")
			}

			s = stateCHR
		}
	}

	// If we got here that means we didn't hit the separator along the way, so the
	// topic is either empty, or does not contain a separator. Either way, we return
	// the full topic
	return topic, "", nil
}

// The QoS of the payload messages sent in response to a subscription must be the
// minimum of the QoS of the originally published message (in this case, it's the
// qos parameter) and the maximum QoS granted by the server (in this case, it's
// the QoS in the topic tree).
//
// It's also possible that even if the topic matches, the subscriber is not included
// due to the QoS granted is lower than the published message QoS. For example,
// if the client is granted only QoS 0, and the publish message is QoS 1, then this
// client is not to be send the published message.
func (sn *sNode) matchQos(qos message.QosType, subs *types.Subscribers) {
	for i, sub := range sn.subs {
		// If the published QoS is higher than the subscriber QoS, then we skip the
		// subscriber. Otherwise, add to the list.
		if qos <= sn.qos[i] {
			sub.WgWriters.Add(1)
			*subs = append(*subs, sub)
		}
	}
}

func equal(k1, k2 interface{}) bool {
	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
		return false
	}

	if reflect.ValueOf(k1).Kind() == reflect.Func {
		return &k1 == &k2
	}

	if k1 == k2 {
		return true
	}

	switch k1 := k1.(type) {
	case string:
		return k1 == k2.(string)
	case int64:
		return k1 == k2.(int64)
	case int32:
		return k1 == k2.(int32)
	case int16:
		return k1 == k2.(int16)
	case int8:
		return k1 == k2.(int8)
	case int:
		return k1 == k2.(int)
	case float32:
		return k1 == k2.(float32)
	case float64:
		return k1 == k2.(float64)
	case uint:
		return k1 == k2.(uint)
	case uint8:
		return k1 == k2.(uint8)
	case uint16:
		return k1 == k2.(uint16)
	case uint32:
		return k1 == k2.(uint32)
	case uint64:
		return k1 == k2.(uint64)
	case uintptr:
		return k1 == k2.(uintptr)
	}
	return false
}
