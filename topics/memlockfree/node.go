package memlockfree

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/vlmonitoring"
	"github.com/VolantMQ/vlapi/vlsubscriber"
	"github.com/VolantMQ/vlapi/vltypes"

	topicsTypes "github.com/VolantMQ/volantmq/topics/types"
)

type topicSubscriber struct {
	s topicsTypes.Subscriber
	p vlsubscriber.SubscriptionParams
	sync.RWMutex
}

func (s *topicSubscriber) acquire() *publish {
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

type retainer struct {
	val interface{}
}

type node struct {
	retained  atomic.Value
	wgDeleted sync.WaitGroup
	subs      sync.Map
	children  sync.Map
	kidsCount int32
	subsCount int32
	remove    int32
	parent    *node
}

func newNode(parent *node) *node {
	n := &node{
		parent: parent,
	}

	n.retained.Store(retainer{})

	return n
}

func (mT *provider) leafInsertNode(levels []string) *node {
	root := mT.root

	for _, level := range levels {
		for {
			atomic.AddInt32(&root.kidsCount, 1)

			value, ok := root.children.LoadOrStore(level, newNode(root))
			n := value.(*node)

			// atomic.AddInt32(&n.subsCount, 1)

			if ok {
				atomic.AddInt32(&root.kidsCount, -1)
				if atomic.LoadInt32(&n.remove) == 1 {
					// atomic.AddInt32(&n.subsCount, -1)
					n.wgDeleted.Wait()
					continue
				}
			}

			// if i != len(levels)-1 {
			// atomic.AddInt32(&n.subsCount, -1)
			// }

			root = n
			break
		}
	}

	return root
}

func (mT *provider) leafSearchNode(levels []string) *node {
	root := mT.root

	// run down and try get path matching given topic
	for _, token := range levels {
		n, ok := root.children.Load(token)
		if !ok {
			return nil
		}

		root = n.(*node)
	}

	return root
}

func (mT *provider) subscriptionInsert(filter string, sub topicsTypes.Subscriber, p vlsubscriber.SubscriptionParams) bool {
	levels := strings.Split(filter, "/")

	leaf := mT.leafInsertNode(levels)

	// Let's see if the subscriber is already on the list and just update QoS if so
	// Otherwise create new entry
	s, ok := leaf.subs.LoadOrStore(sub.Hash(), &topicSubscriber{s: sub, p: p})

	ts := s.(*topicSubscriber)
	if ok {
		ts.Lock()
		ts.p = p
		ts.Unlock()
	} else {
		atomic.AddInt32(&leaf.subsCount, 1)
	}

	return ok
}

func (mT *provider) subscriptionRemove(topic string, sub topicsTypes.Subscriber) error {
	levels := strings.Split(topic, "/")

	var err error

	leaf := mT.leafSearchNode(levels)
	if leaf == nil {
		return topicsTypes.ErrNotFound
	}

	// path matching the topic exists.
	// if subscriber argument is nil remove all of subscribers
	// otherwise try remove subscriber or set error if not exists
	if sub == nil {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		leaf.subs.Range(func(key, value interface{}) bool {
			atomic.AddInt32(&leaf.subsCount, -1)
			leaf.subs.Delete(key)
			return true
		})
	} else {
		if _, ok := leaf.subs.Load(sub.Hash()); !ok {
			err = topicsTypes.ErrNotFound
		} else {
			atomic.AddInt32(&leaf.subsCount, -1)
			leaf.subs.Delete(sub.Hash())
		}
	}

	mT.nodesCleanup(leaf, levels)

	return err
}

func (mT *provider) nodesCleanup(root *node, levels []string) {
	// Run up and on each level and check if level has subscriptions and nested nodes
	// If both are empty tell parent node to remove that token
	level := len(levels)

	for leafNode := root; leafNode != nil; leafNode = leafNode.parent {
		// If there are no more subscribers or inner nodes or retained messages remove this node from parent
		if atomic.LoadInt32(&leafNode.subsCount) == 0 &&
			atomic.LoadInt32(&leafNode.kidsCount) == 0 &&
			leafNode.retained.Load().(retainer).val == nil {
			leafNode.wgDeleted.Add(1)
			atomic.StoreInt32(&leafNode.remove, 1)

			if atomic.LoadInt32(&leafNode.subsCount) == 0 &&
				atomic.LoadInt32(&leafNode.kidsCount) == 0 &&
				leafNode.retained.Load().(retainer).val == nil {
				mT.onCleanUnsubscribe(levels[:level])
				// if this is not root node
				if leafNode.parent != nil {
					leafNode.parent.children.Delete(levels[level-1])
				}
			}

			leafNode.wgDeleted.Done()
		}

		level--
	}
}

func (mT *provider) subscriptionRecurseSearch(root *node, levels []string, publishID uintptr, p *publishes) {
	if len(levels) == 0 {
		// leaf level of the topic
		// get all subscribers and return
		mT.nodeSubscribers(root, publishID, p)
		if n, ok := root.children.Load(topicsTypes.MWC); ok {
			mT.nodeSubscribers(n.(*node), publishID, p)
		}
	} else {
		if n, ok := root.children.Load(topicsTypes.MWC); ok && len(levels[0]) != 0 {
			mT.nodeSubscribers(n.(*node), publishID, p)
		}

		if n, ok := root.children.Load(levels[0]); ok {
			mT.subscriptionRecurseSearch(n.(*node), levels[1:], publishID, p)
		}

		if n, ok := root.children.Load(topicsTypes.SWC); ok {
			mT.subscriptionRecurseSearch(n.(*node), levels[1:], publishID, p)
		}
	}
}

func (mT *provider) subscriptionSearch(topic string, publishID uintptr, p *publishes) {
	root := mT.root
	levels := strings.Split(topic, "/")
	level := levels[0]

	if !strings.HasPrefix(level, "$") {
		mT.subscriptionRecurseSearch(root, levels, publishID, p)
	} else if n, ok := root.children.Load(level); ok {
		mT.subscriptionRecurseSearch(n.(*node), levels[1:], publishID, p)
	}
}

func (mT *provider) retainInsert(topic string, obj vltypes.RetainObject) {
	levels := strings.Split(topic, "/")

	root := mT.leafInsertNode(levels)

	root.retained.Store(retainer{val: obj})
	atomic.AddInt32(&root.subsCount, -1)
}

func (mT *provider) retainRemove(topic string) error {
	levels := strings.Split(topic, "/")

	root := mT.leafSearchNode(levels)
	if root == nil {
		return topicsTypes.ErrNotFound
	}

	root.retained.Store(retainer{})

	mT.nodesCleanup(root, levels)

	return nil
}

func retainRecurseSearch(root *node, levels []string, retained *[]*mqttp.Publish) {
	if len(levels) == 0 {
		// leaf level of the topic
		root.getRetained(retained)
		if value, ok := root.children.Load(topicsTypes.MWC); ok {
			n := value.(*node)
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
			root.children.Range(func(key, value interface{}) bool {
				retainRecurseSearch(value.(*node), levels[1:], retained)

				return true
			})
		default:
			if value, ok := root.children.Load(levels[0]); ok {
				retainRecurseSearch(value.(*node), levels[1:], retained)
			}
		}
	}
}

func (mT *provider) retainSearch(filter string, retained *[]*mqttp.Publish) {
	levels := strings.Split(filter, "/")
	level := levels[0]

	if level == topicsTypes.MWC {
		mT.root.children.Range(func(key, value interface{}) bool {
			t := key.(string)
			n := value.(*node)

			if t != "" && !strings.HasPrefix(t, "$") {
				n.allRetained(retained)
			}

			return true
		})
	} else if strings.HasPrefix(level, "$") {
		value, ok := mT.root.children.Load(level)
		var n *node
		if ok {
			n = value.(*node)
		}
		retainRecurseSearch(n, levels[1:], retained)
	} else {
		retainRecurseSearch(mT.root, levels, retained)
	}
}

func (sn *node) getRetained(retained *[]*mqttp.Publish) {
	rt := sn.retained.Load().(retainer)

	switch val := rt.val.(type) {
	case vltypes.RetainObject:
		var p *mqttp.Publish

		switch t := val.(type) {
		case *mqttp.Publish:
			p = t
		case vlmonitoring.DynamicIFace:
			p = t.Retained()
		default:
			panic("unknown retain type")
		}

		// if publish has expiration set check if there time left to live
		if _, _, expired := p.Expired(); !expired {
			*retained = append(*retained, p)
		} else {
			// publish has expired, thus nobody should get it
			sn.retained.Store(retainer{})
		}
	}
}

func (sn *node) allRetained(retained *[]*mqttp.Publish) {
	sn.getRetained(retained)

	sn.children.Range(func(key, value interface{}) bool {
		n := value.(*node)
		n.allRetained(retained)
		return true
	})
}

func overlappingSubscribers(sn *node, publishID uintptr, p *publishes) {
	sn.subs.Range(func(key, value interface{}) bool {
		id := key.(uintptr)
		sub := value.(*topicSubscriber)

		if s, ok := (*p)[id]; ok {
			if sub.p.ID > 0 {
				s[0].ids = append(s[0].ids, sub.p.ID)
			}

			if s[0].qos < sub.p.Granted {
				s[0].qos = sub.p.Granted
			}
		} else {
			sub.RLock()
			if !sub.p.Ops.NL() || id != publishID {
				if pe := sub.acquire(); pe != nil {
					(*p)[id] = append((*p)[id], pe)
				}
			}
			sub.RUnlock()
		}

		return true
	})
}

func nonOverlappingSubscribers(sn *node, publishID uintptr, p *publishes) {
	sn.subs.Range(func(key, value interface{}) bool {
		id := key.(uintptr)
		sub := value.(*topicSubscriber)

		if !sub.p.Ops.NL() || id != publishID {
			if pe := sub.acquire(); pe != nil {
				if _, ok := (*p)[id]; ok {
					(*p)[id] = append((*p)[id], pe)
				} else {
					(*p)[id] = []*publish{pe}
				}
			}
		}

		return true
	})
}
