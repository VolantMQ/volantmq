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
	"sync"

	"errors"

	"github.com/troian/surgemq"
	"github.com/troian/surgemq/message"
	persistenceTypes "github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics/types"
	"github.com/troian/surgemq/types"
	"go.uber.org/zap"
)

type subscriber struct {
	entry *types.Subscriber
	qos   message.QosType
}

type subscribers map[uintptr]*subscriber

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

	maxQoS message.QosType

	log struct {
		prod *zap.Logger
		dev  *zap.Logger
	}
}

var _ topicsTypes.Provider = (*provider)(nil)

// NewMemProvider returns an new instance of the provider, which is implements the
// TopicsProvider interface. provider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persistend so
// when the server goes, everything will be gone. Use with care.
func NewMemProvider(config *topicsTypes.MemConfig) (topicsTypes.Provider, error) {
	p := &provider{
		sRoot:   newSNode(),
		rRoot:   newRNode(),
		stat:    config.Stat,
		persist: config.Persist,
		maxQoS:  config.MaxQosAllowed,
	}

	p.log.prod = surgemq.GetProdLogger().Named("topics").Named(config.Name)
	p.log.dev = surgemq.GetDevLogger().Named("topics").Named(config.Name)

	if p.persist != nil {
		entries, err := p.persist.Load()
		if err != nil && err != persistenceTypes.ErrNotFound {
			return nil, err
		}

		for _, msg := range entries {
			// Loading retained messages
			if m, ok := msg.(*message.PublishMessage); ok {
				p.log.dev.Debug("Loading retained message", zap.String("topic", m.Topic()), zap.Int8("QoS", int8(m.QoS())))
				p.Retain(m) // nolint: errcheck
			}
		}

		p.persist.Delete() // nolint: errcheck
	}

	return p, nil
}

func (mT *provider) Subscribe(topic string, qos message.QosType, sub *types.Subscriber) (message.QosType, error) {
	if !qos.IsValid() {
		return message.QosFailure, message.ErrInvalidQoS
	}

	if sub == nil {
		return message.QosFailure, topicsTypes.ErrInvalidSubscriber
	}

	if qos < mT.maxQoS {
		qos = mT.maxQoS
	}

	mT.smu.Lock()
	defer mT.smu.Unlock()

	if err := mT.sRoot.insert(topic, qos, sub); err != nil {
		return message.QosFailure, err
	}

	return qos, nil
}

func (mT *provider) Subscribers(topic string, qos message.QosType, subs *types.Subscribers) error {
	mT.smu.RLock()

	if err := mT.sRoot.match(topic, qos, subs); err != nil {
		mT.smu.RUnlock()
		return err
	}
	mT.smu.RUnlock()

	return nil
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

	for _, e := range subs {
		if e != nil {
			if err := e.Publish(msg); err != nil {
				mT.log.prod.Error("Error", zap.Error(err))
			}

			e.WgWriters.Done()
		}
	}

	return nil
}

func (mT *provider) Retain(msg *message.PublishMessage) error {
	mT.rmu.Lock()
	defer mT.rmu.Unlock()

	// [MQTT-3.3.1-10]            [MQTT-3.3.1-7]
	if len(msg.Payload()) == 0 || msg.QoS() == message.QoS0 {
		mT.rRoot.remove(msg.Topic()) // nolint: errcheck, gas

		if len(msg.Payload()) == 0 {
			return nil
		}
	}

	return mT.rRoot.insert(msg.Topic(), msg)
}

func (mT *provider) Retained(topic string) ([]*message.PublishMessage, error) {
	mT.rmu.RLock()
	defer mT.rmu.RUnlock()

	var res []*message.PublishMessage

	// [MQTT-3.3.1-5]
	err := mT.rRoot.match(topic, &res)
	return res, err
}

func (mT *provider) Close() error {
	var res []*message.PublishMessage
	// [MQTT-3.3.1-5]
	if err := mT.rRoot.match("#", &res); err != nil {
		mT.log.prod.Error("Couldn't get retained topics", zap.Error(err))
	}

	toStore := []message.Provider{}

	for _, m := range res {
		toStore = append(toStore, m)
	}

	if mT.persist != nil {
		if len(toStore) > 0 {
			mT.log.dev.Debug("Storing retained messages", zap.Int("amount", len(toStore)))
			mT.persist.Store(toStore) // nolint: errcheck
		}
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
	//stateSEP             // Topic level separator
	stateSYS // System level topic ($)
)

// Returns topic level, remaining topic levels and any errors
func nextTopicLevel(topic string) (string, string, error) {
	s := stateCHR

	for i, c := range topic {
		switch c {
		case '/':
			if s == stateMWC {
				return "", "", topicsTypes.ErrMultiLevel
			}

			if i == 0 {
				return topicsTypes.SWC, topic[i+1:], nil
			}

			return topic[:i], topic[i+1:], nil
		case '#':
			if i != 0 {
				return "", "", topicsTypes.ErrInvalidWildcardPlus
			}

			s = stateMWC

		case '+':
			if i != 0 {
				return "", "", topicsTypes.ErrInvalidWildcardSharp
			}

			s = stateSWC

		case '$':
			if i == 0 {
				return "", "", errors.New("memtopics/nextTopicLevel: Cannot publish to $ topics")
			}

			s = stateSYS

		default:
			if s == stateMWC || s == stateSWC {
				return "", "", topicsTypes.ErrInvalidWildcard
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
	for _, sub := range sn.subs {
		// If the published QoS is higher than the subscriber QoS, then we skip the
		// subscriber. Otherwise, add to the list.
		if qos <= sub.qos {
			sub.entry.WgWriters.Add(1)
			*subs = append(*subs, sub.entry)
		}
	}
}
