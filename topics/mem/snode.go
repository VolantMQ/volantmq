package mem

import (
	"errors"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/topics"
)

// subscription nodes
type sNode struct {
	// If this is the end of the topic string, then add subscribers here
	subs []interface{}
	qos  []message.QosType

	// Otherwise add the next topic level here
	nodes map[string]*sNode
}

func newSNode() *sNode {
	return &sNode{
		nodes: make(map[string]*sNode),
	}
}

func (sn *sNode) insert(topic string, qos message.QosType, sub interface{}) error {
	// If there's no more topic levels, that means we are at the matching sNode
	// to insert the subscriber. So let's see if there's such subscriber,
	// if so, update it. Otherwise insert it.
	if len(topic) == 0 {
		// Let's see if the subscriber is already on the list. If yes, update
		// QoS and then return.
		for i := range sn.subs {
			if equal(sn.subs[i], sub) {
				sn.qos[i] = qos
				return nil
			}
		}

		// Otherwise add.
		sn.subs = append(sn.subs, sub)
		sn.qos = append(sn.qos, qos)

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
func (sn *sNode) remove(topic string, sub interface{}) error {
	// If the topic is empty, it means we are at the final matching snode. If so,
	// let's find the matching subscribers and remove them.
	if len(topic) == 0 {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		if sub == nil {
			sn.subs = sn.subs[0:0]
			sn.qos = sn.qos[0:0]
			return nil
		}

		// If we find the subscriber then remove it from the list. Technically
		// we just overwrite the slot by shifting all other items up by one.
		for i := range sn.subs {
			if equal(sn.subs[i], sub) {
				sn.subs = append(sn.subs[:i], sn.subs[i+1:]...)
				sn.qos = append(sn.qos[:i], sn.qos[i+1:]...)
				return nil
			}
		}

		return errors.New("memtopics/remove: No topic found for subscriber")
	}

	// Not the last level, so let's find the next level snode, and recursively
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
		return errors.New("memtopics/remove: No topic found")
	}

	// Remove the subscriber from the next level snode
	if err := n.remove(rem, sub); err != nil {
		return err
	}

	// If there are no more subscribers and snodes to the next level we just visited
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
func (sn *sNode) match(topic string, qos message.QosType, subs *[]interface{}, qoss *[]message.QosType) error {
	// If the topic is empty, it means we are at the final matching snode. If so,
	// let's find the subscribers that match the qos and append them to the list.
	if len(topic) == 0 {
		sn.matchQos(qos, subs, qoss)
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
		if k == topics.MWC {
			n.matchQos(qos, subs, qoss)
		} else if k == topics.SWC || k == level {
			if err := n.match(rem, qos, subs, qoss); err != nil {
				return err
			}
		}
	}

	return nil
}
