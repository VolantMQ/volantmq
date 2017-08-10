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

	"github.com/troian/surgemq/configuration"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/persistence/types"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics/types"
	"github.com/troian/surgemq/types"
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

	onCleanUnsubscribe func([]string)
	wgPublisher        sync.WaitGroup
	wgPublisherStarted sync.WaitGroup

	inbound    chan *message.PublishMessage
	inRetained chan types.RetainObject

	allowOverlapping bool
}

var _ topicsTypes.Provider = (*provider)(nil)

// NewMemProvider returns an new instance of the provider, which is implements the
// TopicsProvider interface. provider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persistent so
// when the server goes, everything will be gone. Use with care.
func NewMemProvider(config *topicsTypes.MemConfig) (topicsTypes.Provider, error) {
	p := &provider{
		stat:               config.Stat,
		persist:            config.Persist,
		onCleanUnsubscribe: config.OnCleanUnsubscribe,
		inbound:            make(chan *message.PublishMessage, 1024*512),
		inRetained:         make(chan types.RetainObject, 1024*512),
	}
	p.root = newNode(p.allowOverlapping, nil)

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

	p.wgPublisher.Add(1)
	p.wgPublisherStarted.Add(1)
	go p.publisher()
	p.wgPublisherStarted.Wait()

	p.wgPublisher.Add(1)
	p.wgPublisherStarted.Add(1)
	go p.retainer()
	p.wgPublisherStarted.Wait()

	return p, nil
}

func (mT *provider) Subscribe(filter string, q message.QosType, s topicsTypes.Subscriber, id uint32) (message.QosType, []*message.PublishMessage, error) {
	if !q.IsValid() {
		return message.QosFailure, nil, message.ErrInvalidQoS
	}

	if s == nil {
		return message.QosFailure, nil, topicsTypes.ErrInvalidSubscriber
	}

	defer mT.smu.Unlock()
	mT.smu.Lock()

	mT.subscriptionInsert(filter, q, s, id)

	var r []*message.PublishMessage

	// [MQTT-3.3.1-5]
	mT.retainSearch(filter, &r)

	return q, r, nil
}

func (mT *provider) UnSubscribe(topic string, sub topicsTypes.Subscriber) error {
	defer mT.smu.Unlock()
	mT.smu.Lock()

	return mT.subscriptionRemove(topic, sub)
}

func (mT *provider) Publish(msg *message.PublishMessage) error {
	mT.inbound <- msg

	return nil
}

func (mT *provider) Retain(obj types.RetainObject) error {
	mT.inRetained <- obj

	return nil
}

func (mT *provider) Retained(filter string) ([]*message.PublishMessage, error) {
	// [MQTT-3.3.1-5]
	var r []*message.PublishMessage

	defer mT.smu.Unlock()
	mT.smu.Lock()

	// [MQTT-3.3.1-5]
	mT.retainSearch(filter, &r)

	return r, nil
}

func (mT *provider) Close() error {
	defer mT.smu.Unlock()
	mT.smu.Lock()

	close(mT.inbound)
	close(mT.inRetained)

	mT.wgPublisher.Wait()

	var res []*message.PublishMessage
	// [MQTT-3.3.1-5]
	//retainSearch(mT.root, []string{"#"}, &res)

	if mT.persist != nil {
		var encoded [][]byte

		for _, m := range res {
			// Skip retained QoS0 messages
			if m.QoS() != message.QoS0 {
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

func (mT *provider) retain(obj types.RetainObject) {
	insert := true

	mT.smu.Lock()

	switch t := obj.(type) {
	case *message.PublishMessage:
		// [MQTT-3.3.1-10]            [MQTT-3.3.1-7]
		if len(t.Payload()) == 0 || t.QoS() == message.QoS0 {
			mT.retainRemove(obj.Topic()) // nolint: errcheck
			if len(t.Payload()) == 0 {
				insert = false
			}
		}
	}

	if insert {
		mT.retainInsert(obj.Topic(), obj)
	}

	mT.smu.Unlock()
}

func (mT *provider) retainer() {
	defer func() {
		mT.wgPublisher.Done()
	}()
	mT.wgPublisherStarted.Done()

	for obj := range mT.inRetained {
		mT.retain(obj)
	}
}

func (mT *provider) publisher() {
	defer mT.wgPublisher.Done()
	mT.wgPublisherStarted.Done()

	for msg := range mT.inbound {
		pubEntries := publishEntries{}

		mT.smu.Lock()
		mT.subscriptionSearch(msg.Topic(), &pubEntries)

		for _, pub := range pubEntries {
			for _, e := range pub {
				if err := e.s.Publish(msg, e.qos, e.ids); err != nil {
					mT.log.prod.Error("Publish error", zap.Error(err))
				}
				e.s.Release()
			}
		}

		mT.smu.Unlock()
	}
}
