package mem

import (
	"unsafe"

	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/topics/types"
	"github.com/troian/surgemq/types"
)

// subscription nodes
type sNode struct {
	//lock sync.RWMutex

	// If this is the end of the topic string, then add subscribers here
	subs subscribers

	// Otherwise add the next topic level here
	nodes map[string]*sNode
}

func newSNode() *sNode {
	return &sNode{
		subs:  make(subscribers),
		nodes: make(map[string]*sNode),
	}
}

func (sn *sNode) insert(topic string, qos message.QosType, sub *types.Subscriber) error {
	// If there's no more topic levels, that means we are at the matching sNode
	// to insert the subscriber. So let's see if there's such subscriber,
	// if so, update it. Otherwise insert it.
	if len(topic) == 0 {
		// Let's see if the subscriber is already on the list. If yes, update
		// QoS and then return.
		if e, ok := sn.subs[uintptr(unsafe.Pointer(sub))]; ok {
			e.qos = qos
		} else {
			sn.subs[uintptr(unsafe.Pointer(sub))] = &subscriber{
				entry: sub,
				qos:   qos,
			}
		}

		return nil
	}

	// Not the last level, so let's find or create the next level of sNode, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := ntl

	// Add sNode if it doesn't already exist
	n, ok := sn.nodes[level]
	if !ok {
		n = newSNode()
		sn.nodes[level] = n
	}

	return n.insert(rem, qos, sub)
}

// This remove implementation ignores the QoS, as long as the subscriber
// matches then it's removed
func (sn *sNode) remove(topic string, sub *types.Subscriber) error {
	// If the topic is empty, it means we are at the final matching sNode. If so,
	// let's find the matching subscribers and remove them.
	if len(topic) == 0 {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		if sub == nil {
			sn.subs = make(subscribers)
			return nil
		}

		// If we find the subscriber then remove it from the list.
		if _, ok := sn.subs[uintptr(unsafe.Pointer(sub))]; ok {
			delete(sn.subs, uintptr(unsafe.Pointer(sub)))
			return nil
		}

		return types.ErrNotFound
	}

	// Not the last level, so let's find the next level sNode, and recursively
	// call it's remove().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := ntl

	// Find the sNode that matches the topic level
	n, ok := sn.nodes[level]
	if !ok {
		return types.ErrNotFound
	}

	// Remove the subscriber from the next level sNode
	if err := n.remove(rem, sub); err != nil {
		return err
	}

	// If there are no more subscribers and sNodes to the next level we just visited
	// let's remove it
	if len(n.subs) == 0 && len(n.nodes) == 0 {
		delete(sn.nodes, level)
	}

	return nil
}

// match() returns all the subscribers that are subscribed to the topic. Given a topic
// with no wildcards (publish topic), it returns a list of subscribers that subscribes
// to the topic. For each of the level names, it's a match
// - if there are subscribers to '#', then all the subscribers are added to result set
func (sn *sNode) match(topic string, qos message.QosType, subs *types.Subscribers) error {
	// If the topic is empty, it means we are at the final matching sNode. If so,
	// let's find the subscribers that match the qos and append them to the list.
	if len(topic) == 0 {
		sn.matchQos(qos, subs)
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := ntl

	for k, n := range sn.nodes {
		// If the key is "#", then these subscribers are added to the result set
		if k == topicsTypes.MWC {
			n.matchQos(qos, subs)
		} else if k == topicsTypes.SWC || k == level {
			if err := n.match(rem, qos, subs); err != nil {
				return err
			}
		}
	}

	return nil
}
