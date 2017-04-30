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
	"fmt"
	"reflect"
	"sync"

	"errors"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/topics"
)

var (
	// MaxQosAllowed is the maximum QOS supported by this server
	MaxQosAllowed = message.QosExactlyOnce
)

type provider struct {
	// Sub/unSub mutex
	smu sync.RWMutex

	// Subscription tree
	sRoot *sNode

	// Retained message mutex
	rmu sync.RWMutex

	// Retained messages topic tree
	rRoot *rNode
}

//var _ provider = (topics.Provider)(nil)

// NewMemProvider returns an new instance of the provider, which is implements the
// TopicsProvider interface. provider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persistend so
// when the server goes, everything will be gone. Use with care.
func NewMemProvider() topics.Provider {
	return &provider{
		sRoot: newSNode(),
		rRoot: newRNode(),
	}
}

func (mT *provider) Subscribe(topic []byte, qos byte, sub interface{}) (byte, error) {
	if !message.ValidQos(qos) {
		return message.QosFailure, fmt.Errorf("Invalid QoS %d", qos)
	}

	if sub == nil {
		return message.QosFailure, errors.New("Subscriber cannot be nil")
	}

	mT.smu.Lock()
	defer mT.smu.Unlock()

	if qos > MaxQosAllowed {
		qos = MaxQosAllowed
	}

	if err := mT.sRoot.insert(topic, qos, sub); err != nil {
		return message.QosFailure, err
	}

	return qos, nil
}

func (mT *provider) UnSubscribe(topic []byte, sub interface{}) error {
	mT.smu.Lock()
	defer mT.smu.Unlock()

	return mT.sRoot.remove(topic, sub)
}

// Returned values will be invalidated by the next Subscribers call
func (mT *provider) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	if !message.ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}

	mT.smu.RLock()
	defer mT.smu.RUnlock()

	*subs = (*subs)[0:0]
	*qoss = (*qoss)[0:0]

	return mT.sRoot.match(topic, qos, subs, qoss)
}

func (mT *provider) Retain(msg *message.PublishMessage) error {
	mT.rmu.Lock()
	defer mT.rmu.Unlock()

	// So apparently, at least according to the MQTT Conformance/Interoperability
	// Testing, that a payload of 0 means delete the retain message.
	// https://eclipse.org/paho/clients/testing/
	if len(msg.Payload()) == 0 {
		return mT.rRoot.remove(msg.Topic())
	}

	return mT.rRoot.insert(msg.Topic(), msg)
}

func (mT *provider) Retained(topic []byte, msgs *[]*message.PublishMessage) error {
	mT.rmu.RLock()
	defer mT.rmu.RUnlock()

	return mT.rRoot.match(topic, msgs)
}

func (mT *provider) Close() error {
	mT.sRoot = nil
	mT.rRoot = nil
	return nil
}

const (
	stateCHR byte = iota // Regular character
	stateMWC             // Multi-level wildcard
	stateSWC             // Single-level wildcard
	//stateSEP             // Topic level separator
	stateSYS // System level topic ($)
)

// Returns topic level, remaining topic levels and any errors
func nextTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := stateCHR

	for i, c := range topic {
		switch c {
		case '/':
			if s == stateMWC {
				return nil, nil, errors.New("memtopics/nextTopicLevel: Multi-level wildcard found in topic and it's not at the last level")
			}

			if i == 0 {
				return []byte(topics.SWC), topic[i+1:], nil
			}

			return topic[:i], topic[i+1:], nil

		case '#':
			if i != 0 {
				return nil, nil, errors.New("memtopics/nextTopicLevel: Wildcard character '#' must occupy entire topic level")
			}

			s = stateMWC

		case '+':
			if i != 0 {
				return nil, nil, errors.New("memtopics/nextTopicLevel: Wildcard character '+' must occupy entire topic level")
			}

			s = stateSWC

		case '$':
			if i == 0 {
				return nil, nil, errors.New("memtopics/nextTopicLevel: Cannot publish to $ topics")
			}

			s = stateSYS

		default:
			if s == stateMWC || s == stateSWC {
				return nil, nil, errors.New("memtopics/nextTopicLevel: Wildcard characters '#' and '+' must occupy entire topic level")
			}

			s = stateCHR
		}
	}

	// If we got here that means we didn't hit the separator along the way, so the
	// topic is either empty, or does not contain a separator. Either way, we return
	// the full topic
	return topic, nil, nil
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
func (sn *sNode) matchQos(qos byte, subs *[]interface{}, qoss *[]byte) {
	for i, sub := range sn.subs {
		// If the published QoS is higher than the subscriber QoS, then we skip the
		// subscriber. Otherwise, add to the list.
		if qos <= sn.qos[i] {
			*subs = append(*subs, sub)
			*qoss = append(*qoss, qos)
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
