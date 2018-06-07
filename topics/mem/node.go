package mem

import (
	"strings"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/subscriber"
	"github.com/VolantMQ/volantmq/systree"
	"github.com/VolantMQ/volantmq/topics/types"
	"github.com/VolantMQ/volantmq/types"
)

type topicSubscriber struct {
	s topicsTypes.Subscriber
	p *vlsubscriber.SubscriptionParams
}

type subscribers map[uintptr]*topicSubscriber

func (s *topicSubscriber) acquire() *publish {
	s.s.Acquire()
	pe := &publish{
		s:   s.s,
		qos: s.p.Granted,
		ops: s.p.Ops,
	}

	if s.p.ID > 0 {
		pe.ids = []uint32{s.p.ID}
	}

	return pe
}

type publish struct {
	s   topicsTypes.Subscriber
	ops mqttp.SubscriptionOptions
	qos mqttp.QosType
	ids []uint32
}

type publishes map[uintptr][]*publish

type node struct {
	retained       interface{}
	subs           subscribers
	parent         *node
	children       map[string]*node
	getSubscribers func(uintptr, *publishes)
}

func newNode(overlap bool, parent *node) *node {
	n := &node{
		subs:     make(subscribers),
		children: make(map[string]*node),
		parent:   parent,
	}

	if overlap {
		n.getSubscribers = n.overlappingSubscribers
	} else {
		n.getSubscribers = n.nonOverlappingSubscribers
	}

	return n
}

func (mT *provider) leafInsertNode(levels []string) *node {
	root := mT.root
	for _, level := range levels {
		// Add node if it doesn't already exist
		node, ok := root.children[level]
		if !ok {
			node = newNode(mT.allowOverlapping, root)

			root.children[level] = node
		}

		root = node
	}

	return root
}

func (mT *provider) leafSearchNode(levels []string) *node {
	root := mT.root

	// run down and try get path matching given topic
	for _, token := range levels {
		n, ok := root.children[token]
		if !ok {
			return nil
		}

		root = n
	}

	return root
}

func (mT *provider) subscriptionInsert(filter string, sub topicsTypes.Subscriber, p *vlsubscriber.SubscriptionParams) bool {
	levels := strings.Split(filter, "/")

	root := mT.leafInsertNode(levels)

	// Let's see if the subscriber is already on the list and just update QoS if so
	// Otherwise create new entry
	exists := false
	if s, ok := root.subs[sub.Hash()]; !ok {
		root.subs[sub.Hash()] = &topicSubscriber{
			s: sub,
			p: p,
		}
	} else {
		s.p = p
		exists = true
	}

	return exists
}

func (mT *provider) subscriptionRemove(topic string, sub topicsTypes.Subscriber) error {
	levels := strings.Split(topic, "/")

	var err error

	root := mT.leafSearchNode(levels)
	if root == nil {
		return topicsTypes.ErrNotFound
	}

	// path matching the topic exists.
	// if subscriber argument is nil remove all of subscribers
	// otherwise try remove subscriber or set error if not exists
	if sub == nil {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		root.subs = make(subscribers)
	} else {
		id := sub.Hash()
		if _, ok := root.subs[id]; ok {
			delete(root.subs, id)
		} else {
			err = topicsTypes.ErrNotFound
		}
	}

	// Run up and on each level and check if level has subscriptions and nested nodes
	// If both are empty tell parent node to remove that token
	level := len(levels)
	for leafNode := root; leafNode != nil; leafNode = leafNode.parent {
		// If there are no more subscribers or inner nodes or retained messages remove this node from parent
		if len(leafNode.subs) == 0 && len(leafNode.children) == 0 && leafNode.retained == nil {
			// if this is not root node
			mT.onCleanUnsubscribe(levels[:level])
			if leafNode.parent != nil {
				delete(leafNode.parent.children, levels[level-1])
			}
		}

		level--
	}

	return err
}

func subscriptionRecurseSearch(root *node, levels []string, publishID uintptr, p *publishes) {
	if len(levels) == 0 {
		// leaf level of the topic
		// get all subscribers and return
		root.getSubscribers(publishID, p)
		if n, ok := root.children[topicsTypes.MWC]; ok {
			n.getSubscribers(publishID, p)
		}
	} else {
		if n, ok := root.children[topicsTypes.MWC]; ok && len(levels[0]) != 0 {
			n.getSubscribers(publishID, p)
		}

		if n, ok := root.children[levels[0]]; ok {
			subscriptionRecurseSearch(n, levels[1:], publishID, p)
		}

		if n, ok := root.children[topicsTypes.SWC]; ok {
			subscriptionRecurseSearch(n, levels[1:], publishID, p)
		}
	}
}

func (mT *provider) subscriptionSearch(topic string, publishID uintptr, p *publishes) {
	root := mT.root
	levels := strings.Split(topic, "/")
	level := levels[0]

	if !strings.HasPrefix(level, "$") {
		subscriptionRecurseSearch(root, levels, publishID, p)
	} else if n, ok := root.children[level]; ok {
		subscriptionRecurseSearch(n, levels[1:], publishID, p)
	}
}

func (mT *provider) retainInsert(topic string, obj types.RetainObject) {
	levels := strings.Split(topic, "/")

	root := mT.leafInsertNode(levels)

	root.retained = obj
}

func (mT *provider) retainRemove(topic string) error {
	levels := strings.Split(topic, "/")

	root := mT.leafSearchNode(levels)
	if root == nil {
		return topicsTypes.ErrNotFound
	}

	root.retained = nil

	// Run up and on each level and check if level has subscriptions and nested nodes
	// If both are empty tell parent node to remove that token
	level := len(levels)
	for leafNode := root; leafNode != nil; leafNode = leafNode.parent {
		// If there are no more subscribers or inner nodes or retained messages remove this node from parent
		if len(leafNode.subs) == 0 && len(leafNode.children) == 0 && leafNode.retained == nil {
			// if this is not root node
			if leafNode.parent != nil {
				delete(leafNode.parent.children, levels[level-1])
			}
		}

		level--
	}

	return nil
}

func retainRecurseSearch(root *node, levels []string, retained *[]*mqttp.Publish) {
	if len(levels) == 0 {
		// leaf level of the topic
		root.getRetained(retained)
		if n, ok := root.children[topicsTypes.MWC]; ok {
			n.allRetained(retained)
		}
	} else {
		switch levels[0] {
		case topicsTypes.MWC:
			// If '#', add all retained messages starting this node
			root.allRetained(retained)
			return
		case topicsTypes.SWC:
			// If '+', check all nodes at this level. Next levels must be matched.
			for _, n := range root.children {
				retainRecurseSearch(n, levels[1:], retained)
			}
		default:
			if n, ok := root.children[levels[0]]; ok {
				retainRecurseSearch(n, levels[1:], retained)
			}
		}
	}
}

func (mT *provider) retainSearch(filter string, retained *[]*mqttp.Publish) {
	levels := strings.Split(filter, "/")
	level := levels[0]

	if level == topicsTypes.MWC {
		for t, n := range mT.root.children {
			if t != "" && !strings.HasPrefix(t, "$") {
				n.allRetained(retained)
			}
		}
	} else if strings.HasPrefix(level, "$") && mT.root.children[level] != nil {
		retainRecurseSearch(mT.root.children[level], levels[1:], retained)
	} else {
		retainRecurseSearch(mT.root, levels, retained)
	}
}

func (sn *node) getRetained(retained *[]*mqttp.Publish) {
	if sn.retained != nil {
		var p *mqttp.Publish

		switch t := sn.retained.(type) {
		case *mqttp.Publish:
			p = t
		case systree.DynamicValue:
			p = t.Retained()
		}

		// if publish has expiration set check if there time left to live
		if _, _, expired := p.Expired(); !expired {
			*retained = append(*retained, p)
		} else {
			// publish has expired, thus nobody should get it
			sn.retained = nil
		}
	}
}

func (sn *node) allRetained(retained *[]*mqttp.Publish) {
	sn.getRetained(retained)

	for _, n := range sn.children {
		n.allRetained(retained)
	}
}

func (sn *node) overlappingSubscribers(publishID uintptr, p *publishes) {
	for id, sub := range sn.subs {
		if s, ok := (*p)[id]; ok {
			if sub.p.ID > 0 {
				s[0].ids = append(s[0].ids, sub.p.ID)
			}

			if s[0].qos < sub.p.Granted {
				s[0].qos = sub.p.Granted
			}
		} else {
			if !sub.p.Ops.NL() || id != publishID {
				pe := sub.acquire()
				(*p)[id] = append((*p)[id], pe)
			}
		}
	}
}

func (sn *node) nonOverlappingSubscribers(publishID uintptr, p *publishes) {
	for id, sub := range sn.subs {
		if !sub.p.Ops.NL() || id != publishID {
			pe := sub.acquire()
			if _, ok := (*p)[id]; ok {
				(*p)[id] = append((*p)[id], pe)
			} else {
				(*p)[id] = []*publish{pe}
			}
		}
	}
}
