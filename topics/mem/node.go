package mem

import (
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/topics/types"
)

// subscriptionEntry
type entry struct {
	s          topicsTypes.Subscriber
	grantedQoS message.QosType
}

type entries []*entry
type subEntries map[uintptr]*entry

type node struct {
	retained *message.PublishMessage
	subs     subEntries
	parent   *node
	children map[string]*node
}

func newSNode(parent *node) *node {
	return &node{
		subs:     make(subEntries),
		children: make(map[string]*node),
		parent:   parent,
	}
}

func retainInsert(root *node, levels []string, msg *message.PublishMessage) {
	for _, token := range levels {
		// Add node if it doesn't already exist
		node, ok := root.children[token]
		if !ok {
			node = newSNode(root)
			root.children[token] = node
		}
		root = node
	}

	root.retained = msg
}

func subscriptionInsert(root *node, levels []string, qos message.QosType, sub topicsTypes.Subscriber) {
	for _, token := range levels {
		// Add node if it doesn't already exist
		node, ok := root.children[token]
		if !ok {
			node = newSNode(root)

			root.children[token] = node
		}
		root = node
	}

	// Let's see if the subscriber is already on the list and just update QoS if so
	// Otherwise create new entry
	if s, ok := root.subs[sub.Hash()]; !ok {
		root.subs[sub.Hash()] = &entry{
			s:          sub,
			grantedQoS: qos,
		}
	} else {
		s.grantedQoS = qos
	}
}

func retainRemove(root *node, levels []string) error {
	// run down and try get path matching given topic
	for _, token := range levels {
		n, ok := root.children[token]
		if !ok {
			return topicsTypes.ErrNotFound
		}

		root = n
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

func subscriptionRemove(root *node, levels []string, sub topicsTypes.Subscriber) error {
	//levels := strings.Split(topic, "/")

	var err error

	// run down and try get path matching given topic
	for _, token := range levels {
		n, ok := root.children[token]
		if !ok {
			return topicsTypes.ErrNotFound
		}

		root = n
	}

	// path matching the topic exists.
	// if subscriber argument is nil remove all of subscribers
	// otherwise try remove subscriber or set error if not exists
	if sub == nil {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		root.subs = make(subEntries)
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
			if leafNode.parent != nil {
				delete(leafNode.parent.children, levels[level-1])
			}
		}

		level--
	}

	return err
}

func retainSearch(root *node, levels []string, retained *[]*message.PublishMessage) {
	if len(levels) == 0 {
		// leaf level of the topic
		if root.retained != nil {
			*retained = append(*retained, root.retained)
		}
		if n, ok := root.children[topicsTypes.MWC]; ok {
			n.allRetained(retained)
		}
	} else {
		switch levels[0] {
		case topicsTypes.MWC:
			// If '#', add all retained messages starting this node
			root.allRetained(retained)
		case topicsTypes.SWC:
			// If '+', check all nodes at this level. Next levels must be matched.
			for _, n := range root.children {
				retainSearch(n, levels[1:], retained)
			}
		default:
			if n, ok := root.children[levels[0]]; ok {
				retainSearch(n, levels[1:], retained)
			}
		}
	}
}
func subscriptionSearch(root *node, levels []string, subs *entries) {
	if len(levels) == 0 {
		// leaf level of the topic
		// get all subscribers and return
		*subs = append(*subs, root.sliceSubscribers()...)
		if n, ok := root.children[topicsTypes.MWC]; ok {
			*subs = append(*subs, n.sliceSubscribers()...)
		}
	} else {
		if n, ok := root.children[topicsTypes.MWC]; ok && len(levels[0]) != 0 {
			*subs = append(*subs, n.sliceSubscribers()...)
		}

		if n, ok := root.children[levels[0]]; ok {
			subscriptionSearch(n, levels[1:], subs)
		}

		if n, ok := root.children[topicsTypes.SWC]; ok {
			subscriptionSearch(n, levels[1:], subs)
		}
	}
}

func (sn *node) allRetained(retained *[]*message.PublishMessage) {
	if sn.retained != nil {
		*retained = append(*retained, sn.retained)
	}

	for _, n := range sn.children {
		n.allRetained(retained)
	}
}

func (sn *node) sliceSubscribers() entries {
	var res entries
	for _, sub := range sn.subs {
		// If the published QoS is higher than the subscriber QoS, then we skip the
		// subscriber. Otherwise, add to the list.
		sub.s.Acquire()
		res = append(res, sub)
	}

	return res
}
