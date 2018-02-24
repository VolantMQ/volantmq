// Copyright (c) 2014 The VolantMQ Authors. All rights reserved.
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
	"time"

	"github.com/VolantMQ/persistence"
	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/topics/types"
	"github.com/VolantMQ/volantmq/types"
	"go.uber.org/zap"
)

type provider struct {
	smu                sync.RWMutex
	root               *node
	stat               systree.TopicsStat
	persist            persistence.Retained
	log                *zap.SugaredLogger
	onCleanUnsubscribe func([]string)
	wgPublisher        sync.WaitGroup
	wgPublisherStarted sync.WaitGroup
	inbound            chan *packet.Publish
	inRetained         chan types.RetainObject
	allowOverlapping   bool
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
		inbound:            make(chan *packet.Publish, 1024*512),
		inRetained:         make(chan types.RetainObject, 1024*512),
	}
	p.root = newNode(p.allowOverlapping, nil)

	p.log = configuration.GetLogger().Named("topics").Named(config.Name)

	if p.persist != nil {
		entries, err := p.persist.Load()
		if err != nil && err != persistence.ErrNotFound {
			return nil, err
		}

		for _, d := range entries {
			v := packet.ProtocolVersion(d.Data[0])
			pkt, _, err := packet.Decode(v, d.Data[1:])
			if err != nil {
				p.log.Error("Couldn't decode retained message", zap.Error(err))
			} else {
				if m, ok := pkt.(*packet.Publish); ok {
					if len(d.ExpireAt) > 0 {
						if tm, err := time.Parse(time.RFC3339, d.ExpireAt); err == nil {
							m.SetExpireAt(tm)
						} else {
							p.log.Error("Decode publish expire at", zap.Error(err))
						}
					}
					p.Retain(m) // nolint: errcheck
				} else {
					p.log.Warn("Unsupported retained message type", zap.String("type", m.Type().Name()))
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

func (mT *provider) Subscribe(filter string, s topicsTypes.Subscriber, p *topicsTypes.SubscriptionParams) (packet.QosType, []*packet.Publish, error) {
	defer mT.smu.Unlock()
	mT.smu.Lock()

	p.Granted = p.Ops.QoS()
	exists := mT.subscriptionInsert(filter, s, p)

	var r []*packet.Publish

	// [MQTT-3.3.1-5]
	rh := p.Ops.RetainHandling()
	if (rh == packet.RetainHandlingRetain) || ((rh == packet.RetainHandlingIfNotExists) && !exists) {
		mT.retainSearch(filter, &r)
	}

	return p.Granted, r, nil
}

func (mT *provider) UnSubscribe(topic string, sub topicsTypes.Subscriber) error {
	defer mT.smu.Unlock()
	mT.smu.Lock()

	return mT.subscriptionRemove(topic, sub)
}

func (mT *provider) Publish(m interface{}) error {
	msg, ok := m.(*packet.Publish)
	if !ok {
		return topicsTypes.ErrUnexpectedObjectType
	}
	mT.inbound <- msg

	return nil
}

func (mT *provider) Retain(obj types.RetainObject) error {
	mT.inRetained <- obj

	return nil
}

func (mT *provider) Retained(filter string) ([]*packet.Publish, error) {
	// [MQTT-3.3.1-5]
	var r []*packet.Publish

	defer mT.smu.Unlock()
	mT.smu.Lock()

	// [MQTT-3.3.1-5]
	mT.retainSearch(filter, &r)

	return r, nil
}

func (mT *provider) Close() error {
	close(mT.inbound)
	close(mT.inRetained)

	mT.wgPublisher.Wait()

	defer mT.smu.Unlock()
	mT.smu.Lock()

	if mT.persist != nil {
		var res []*packet.Publish
		// [MQTT-3.3.1-5]
		mT.retainSearch("#", &res)
		mT.retainSearch("/#", &res)
		mT.retainSearch("$share/#", &res)

		var encoded persistence.PersistedPackets

		for _, pkt := range res {
			// Discard retained expired and QoS0 messages
			if expireAt, _, expired := pkt.Expired(); !expired && pkt.QoS() != packet.QoS0 {
				if buf, err := packet.Encode(pkt); err != nil {
					mT.log.Error("Couldn't encode retained message", zap.Error(err))
				} else {
					entry := &persistence.PersistedPacket{
						Data: buf,
					}
					if !expireAt.IsZero() {
						entry.ExpireAt = expireAt.Format(time.RFC3339)
					}
					encoded = append(encoded, entry)
				}
			}
		}
		if len(encoded) > 0 {
			mT.log.Debug("Storing retained messages", zap.Int("amount", len(encoded)))
			if err := mT.persist.Store(encoded); err != nil {
				mT.log.Error("Couldn't persist retained messages", zap.Error(err))
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
	case *packet.Publish:
		// [MQTT-3.3.1-10]            [MQTT-3.3.1-7]
		if len(t.Payload()) == 0 || t.QoS() == packet.QoS0 {
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
		pubEntries := publishes{}

		mT.smu.Lock()
		mT.subscriptionSearch(msg.Topic(), msg.PublishID(), &pubEntries)

		for _, pub := range pubEntries {
			for _, e := range pub {
				if err := e.s.Publish(msg, e.qos, e.ops, e.ids); err != nil {
					mT.log.Error("Publish error", zap.Error(err))
				}
				e.s.Release()
			}
		}

		mT.smu.Unlock()
	}
}
