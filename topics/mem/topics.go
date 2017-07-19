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

	"strings"

	"github.com/troian/surgemq/configuration"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics/types"
	"go.uber.org/zap"
)

type provider struct {
	// Sub/unSub mutex
	smu sync.RWMutex

	// Subscription tree
	root *node

	stat systree.TopicsStat

	persist persistenceTypes.Retained

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
		root:    newSNode(nil),
		stat:    config.Stat,
		persist: config.Persist,
	}

	p.log.prod = configuration.GetProdLogger().Named("topics").Named(config.Name)
	p.log.dev = configuration.GetDevLogger().Named("topics").Named(config.Name)

	if p.persist != nil {
		entries, err := p.persist.Load()
		if err != nil && err != persistenceTypes.ErrNotFound {
			return nil, err
		}

		for _, d := range entries {
			v := message.ProtocolVersion(d[0])
			msg, _, err := message.Decode(v, d[1:])
			if err != nil {
				p.log.prod.Error("Couldn't decode retained message", zap.Error(err))
			} else {
				if m, ok := msg.(*message.PublishMessage); ok {
					p.log.dev.Debug("Loading retained message",
						zap.String("topic", m.Topic()),
						zap.Int8("QoS", int8(m.QoS())))

					p.Retain(m) // nolint: errcheck
				} else {
					p.log.prod.Warn("Unsupported retained message type", zap.String("type", m.Type().Name()))
				}
			}
		}
	}

	return p, nil
}

func (mT *provider) Subscribe(topic string, qos message.QosType, sub topicsTypes.Subscriber) (message.QosType, []*message.PublishMessage, error) {
	if !qos.IsValid() {
		return message.QosFailure, nil, message.ErrInvalidQoS
	}

	if sub == nil {
		return message.QosFailure, nil, topicsTypes.ErrInvalidSubscriber
	}

	levels := strings.Split(topic, "/")

	defer mT.smu.Unlock()
	mT.smu.Lock()

	subscriptionInsert(mT.root, levels, qos, sub)

	var r []*message.PublishMessage

	// [MQTT-3.3.1-5]
	retainSearch(mT.root, levels, &r)

	return qos, r, nil
}

func (mT *provider) UnSubscribe(topic string, sub topicsTypes.Subscriber) error {
	levels := strings.Split(topic, "/")

	defer mT.smu.Unlock()
	mT.smu.Lock()

	return subscriptionRemove(mT.root, levels, sub)
}

func (mT *provider) Publish(msg *message.PublishMessage) error {
	levels := strings.Split(msg.Topic(), "/")
	var subs entries

	defer mT.smu.Unlock()
	mT.smu.Lock()

	subscriptionSearch(mT.root, levels, &subs)

	for _, s := range subs {
		if err := s.s.Publish(msg, s.grantedQoS); err != nil {
			mT.log.prod.Error("Publish error", zap.Error(err))
		}
		s.s.Release()
	}

	return nil
}

func (mT *provider) Retain(msg *message.PublishMessage) error {
	levels := strings.Split(msg.Topic(), "/")

	defer mT.smu.Unlock()
	mT.smu.Lock()

	// [MQTT-3.3.1-10]            [MQTT-3.3.1-7]
	if len(msg.Payload()) == 0 || msg.QoS() == message.QoS0 {
		retainRemove(mT.root, levels) // nolint: errcheck
		if len(msg.Payload()) == 0 {
			return nil
		}
	}

	retainInsert(mT.root, levels, msg)

	return nil
}

func (mT *provider) Retained(topic string) ([]*message.PublishMessage, error) {
	levels := strings.Split(topic, "/")

	// [MQTT-3.3.1-5]
	var r []*message.PublishMessage

	defer mT.smu.Unlock()
	mT.smu.Lock()

	// [MQTT-3.3.1-5]
	retainSearch(mT.root, levels, &r)

	return r, nil
}

func (mT *provider) Close() error {
	defer mT.smu.Unlock()
	mT.smu.Lock()

	var res []*message.PublishMessage
	// [MQTT-3.3.1-5]
	retainSearch(mT.root, []string{"#"}, &res)

	if mT.persist != nil {
		var encoded [][]byte

		for _, m := range res {
			if sz, err := m.Size(); err != nil {
				mT.log.prod.Error("Couldn't get retained message size", zap.Error(err))
			} else {
				buf := make([]byte, sz)
				if _, err = m.Encode(buf); err != nil {
					mT.log.prod.Error("Couldn't encode retained message", zap.Error(err))
				} else {
					encoded = append(encoded, buf)
				}
			}
		}
		if len(encoded) > 0 {
			mT.log.dev.Debug("Storing retained messages", zap.Int("amount", len(encoded)))
			if err := mT.persist.Store(encoded); err != nil {
				mT.log.prod.Error("Couldn't persist retained messages", zap.Error(err))
			}
		}
	}

	mT.root = nil
	return nil
}
