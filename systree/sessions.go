package systree

import (
	"encoding/json"
	"sync/atomic"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/volantmq/types"
)

// SessionCreatedStatus report when session status once created
type SessionCreatedStatus struct {
	ExpiryInterval string `json:"expiryInterval,omitempty"`
	WillDelay      string `json:"willDelay,omitempty"`
	Timestamp      string `json:"timestamp"`
	Clean          bool   `json:"clean"`
	Durable        bool   `json:"durable"`
}

// SessionDeletedStatus report when session status once deleted
type SessionDeletedStatus struct {
	Timestamp string `json:"timestamp"`
	Reason    string `json:"reason"`
}

type sessions struct {
	stat

	topicsManager types.TopicMessenger
	topic         string
}

func newSessions(topicPrefix string, retained *[]types.RetainObject) sessions {
	c := sessions{
		stat:  newStat(topicPrefix+"/stats/sessions", retained),
		topic: topicPrefix + "/sessions/",
	}

	return c
}

// Created add to statistic new client
func (t *sessions) Created(id string, status *SessionCreatedStatus) {
	newVal := atomic.AddUint64(&t.curr.val, 1)
	if atomic.LoadUint64(&t.max.val) < newVal {
		atomic.StoreUint64(&t.max.val, newVal)
	}

	if t.topicsManager != nil {
		// notify client connected
		nm, _ := mqttp.New(mqttp.ProtocolV311, mqttp.PUBLISH)
		notifyMsg, _ := nm.(*mqttp.Publish)
		notifyMsg.SetRetain(false)
		notifyMsg.SetQoS(mqttp.QoS0)     // nolint: errcheck
		notifyMsg.SetTopic(t.topic + id) // nolint: errcheck

		if out, err := json.Marshal(&status); err != nil {
			// todo: put reliable message
			notifyMsg.SetPayload([]byte("data error"))
		} else {
			notifyMsg.SetPayload(out)
		}

		t.topicsManager.Publish(notifyMsg) // nolint: errcheck
		t.topicsManager.Retain(notifyMsg)  // nolint: errcheck
	}
}

// Removed remove client from statistic
func (t *sessions) Removed(id string, status *SessionDeletedStatus) {
	atomic.AddUint64(&t.curr.val, ^uint64(0))
	if t.topicsManager != nil {
		nm, _ := mqttp.New(mqttp.ProtocolV311, mqttp.PUBLISH)
		notifyMsg, _ := nm.(*mqttp.Publish)
		notifyMsg.SetRetain(false)
		notifyMsg.SetQoS(mqttp.QoS0)     // nolint: errcheck
		notifyMsg.SetTopic(t.topic + id) // nolint: errcheck

		t.topicsManager.Retain(notifyMsg) // nolint: errcheck

		nm, _ = mqttp.New(mqttp.ProtocolV311, mqttp.PUBLISH)
		notifyMsg, _ = nm.(*mqttp.Publish)
		notifyMsg.SetRetain(false)
		notifyMsg.SetQoS(mqttp.QoS0)                  // nolint: errcheck
		notifyMsg.SetTopic(t.topic + id)              // nolint: errcheck
		notifyMsg.SetTopic(t.topic + id + "/removed") // nolint: errcheck
		if out, err := json.Marshal(&status); err != nil {
			notifyMsg.SetPayload([]byte("data error"))
		} else {
			notifyMsg.SetPayload(out)
		}

		t.topicsManager.Publish(notifyMsg) // nolint: errcheck
	}
}
