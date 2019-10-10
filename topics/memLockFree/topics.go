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

package memLockFree

import (
	"sync"
	"time"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/vlplugin/vlpersistence"
	"go.uber.org/zap"

	"github.com/VolantMQ/volantmq/configuration"
	"github.com/VolantMQ/volantmq/systree"
	topicsTypes "github.com/VolantMQ/volantmq/topics/types"
	"github.com/VolantMQ/volantmq/types"
)

type provider struct {
	// smu                sync.RWMutex
	root               *node
	stat               systree.TopicsStat
	persist            vlpersistence.Retained
	log                *zap.SugaredLogger
	wgPublisher        sync.WaitGroup
	wgPublisherStarted sync.WaitGroup
	inbound            chan *mqttp.Publish
	inRetained         chan types.RetainObject
	subIn              chan topicsTypes.SubscribeReq
	unSubIn            chan topicsTypes.UnSubscribeReq
	onCleanUnsubscribe func([]string)
	nodeSubscribers    func(sn *node, publishID uintptr, p *publishes)
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
		inbound:            make(chan *mqttp.Publish, 1024*512),
		inRetained:         make(chan types.RetainObject, 1024*512),
		subIn:              make(chan topicsTypes.SubscribeReq, 1024*512),
		unSubIn:            make(chan topicsTypes.UnSubscribeReq, 1024*512),
	}

	if config.OverlappingSubscriptions {
		p.nodeSubscribers = overlappingSubscribers
	} else {
		p.nodeSubscribers = nonOverlappingSubscribers
	}

	p.root = newNode(nil)

	p.log = configuration.GetLogger().Named("topics").Named(config.Name)

	if p.persist != nil {
		entries, err := p.persist.Load()
		if err != nil && err != vlpersistence.ErrNotFound {
			return nil, err
		}

		for _, d := range entries {
			var pkt mqttp.IFace
			pkt, _, err = mqttp.Decode(mqttp.ProtocolV50, d.Data)
			if err != nil {
				p.log.Error("Couldn't decode retained message", zap.Error(err))
			} else {
				if m, ok := pkt.(*mqttp.Publish); ok {
					if len(d.ExpireAt) > 0 {
						var tm time.Time
						if tm, err = time.Parse(time.RFC3339, d.ExpireAt); err == nil {
							m.SetExpireAt(tm)
						} else {
							p.log.Error("Decode publish expire at", zap.Error(err))
						}
					}
					_ = p.Retain(m) // nolint: errcheck
				} else {
					p.log.Warn("Unsupported retained message type", zap.String("type", m.Type().Name()))
				}
			}
		}
	}

	publisherCount := 2
	subsCount := 2
	unSunCount := 2

	p.wgPublisher.Add(publisherCount + subsCount + unSunCount + 1)
	p.wgPublisherStarted.Add(publisherCount + subsCount + unSunCount + 1)

	for i := 0; i < publisherCount; i++ {
		go p.publisher()
	}

	go p.retainer()

	for i := 0; i < subsCount; i++ {
		go p.subscriber()
	}

	for i := 0; i < unSunCount; i++ {
		go p.unSubscriber()
	}

	p.wgPublisherStarted.Wait()

	return p, nil
}

func (mT *provider) Subscribe(req topicsTypes.SubscribeReq) topicsTypes.SubscribeResp {
	cAllocated := false

	if req.Chan == nil {
		cAllocated = true
		req.Chan = make(chan topicsTypes.SubscribeResp)
	}

	mT.subIn <- req

	resp := <-req.Chan

	if cAllocated {
		close(req.Chan)
	}

	return resp
}

func (mT *provider) UnSubscribe(req topicsTypes.UnSubscribeReq) topicsTypes.UnSubscribeResp {
	cAllocated := false

	if req.Chan == nil {
		cAllocated = true
		req.Chan = make(chan topicsTypes.UnSubscribeResp)
	}

	mT.unSubIn <- req

	resp := <-req.Chan

	if cAllocated {
		close(req.Chan)
	}

	return resp
}

func (mT *provider) Publish(m interface{}) error {
	msg, ok := m.(*mqttp.Publish)
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

func (mT *provider) Retained(filter string) ([]*mqttp.Publish, error) {
	var r []*mqttp.Publish

	// [MQTT-3.3.1-5]
	mT.retainSearch(filter, &r)

	return r, nil
}

func (mT *provider) Shutdown() error {
	close(mT.inbound)
	close(mT.inRetained)
	close(mT.subIn)
	close(mT.unSubIn)

	mT.wgPublisher.Wait()

	if mT.persist != nil {
		var res []*mqttp.Publish
		// [MQTT-3.3.1-5]
		mT.retainSearch("#", &res)
		mT.retainSearch("/#", &res)

		var encoded []*vlpersistence.PersistedPacket

		for _, pkt := range res {
			// Discard retained expired and QoS0 messages
			if expireAt, _, expired := pkt.Expired(); !expired && pkt.QoS() != mqttp.QoS0 {
				if buf, err := mqttp.Encode(pkt); err != nil {
					mT.log.Error("Couldn't encode retained message", zap.Error(err))
				} else {
					entry := &vlpersistence.PersistedPacket{
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

	switch t := obj.(type) {
	case *mqttp.Publish:
		// [MQTT-3.3.1-10]            [MQTT-3.3.1-7]
		if len(t.Payload()) == 0 || t.QoS() == mqttp.QoS0 {
			_ = mT.retainRemove(obj.Topic()) // nolint: errcheck
			if len(t.Payload()) == 0 {
				insert = false
			}
		}
	}

	if insert {
		mT.retainInsert(obj.Topic(), obj)
	}
}

func (mT *provider) subscriber() {
	defer mT.wgPublisher.Done()
	mT.wgPublisherStarted.Done()

	for req := range mT.subIn {
		var resp topicsTypes.SubscribeResp

		if req.S == nil || req.Params == nil {
			resp.Err = topicsTypes.ErrInvalidArgs
		} else {
			if req.Params.Ops.QoS() > mqttp.QoS2 {
				resp.Err = mqttp.ErrInvalidQoS
			} else {
				resp.Granted = req.Params.Ops.QoS()
				exists := mT.subscriptionInsert(req.Filter, req.S, req.Params)

				var r []*mqttp.Publish

				// [MQTT-3.3.1-5]
				rh := req.Params.Ops.RetainHandling()
				if (rh == mqttp.RetainHandlingRetain) || ((rh == mqttp.RetainHandlingIfNotExists) && !exists) {
					mT.retainSearch(req.Filter, &r)
				}

				resp.Retained = r
			}
		}

		req.Chan <- resp
	}
}

func (mT *provider) unSubscriber() {
	defer mT.wgPublisher.Done()
	mT.wgPublisherStarted.Done()

	for req := range mT.unSubIn {
		req.Chan <- topicsTypes.UnSubscribeResp{Err: mT.subscriptionRemove(req.Filter, req.S)}
	}
}

func (mT *provider) retainer() {
	defer mT.wgPublisher.Done()
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

		mT.subscriptionSearch(msg.Topic(), msg.PublishID(), &pubEntries)

		for _, pub := range pubEntries {
			for _, e := range pub {
				if err := e.s.Publish(msg, e.qos, e.ops, e.ids); err != nil {
					mT.log.Error("Publish error", zap.Error(err))
				}
				e.s.Release()
			}
		}
	}
}
