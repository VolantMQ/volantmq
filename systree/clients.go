package systree

import (
	"sync/atomic"
	"time"

	"encoding/json"

	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/types"
)

type ClientConnectStatus struct {
	Address        string
	Username       string
	CleanSession   bool
	SessionPresent bool
	Protocol       message.ProtocolVersion
	Timestamp      string
	ConnAckCode    message.ReasonCode
}

type clientDisconnectStatus struct {
	Reason    string
	Timestamp string
}

type clients struct {
	stat
	topic         string
	topicsManager Topics
}

func newClients(topicPrefix string, retained *[]types.RetainObject) clients {
	c := clients{
		stat:  newStat(topicPrefix+"/stats/clients", retained),
		topic: topicPrefix + "/clients/",
	}

	return c
}

// Connected add to statistic new client
func (t *clients) Connected(id string, status *ClientConnectStatus) {
	newVal := atomic.AddUint64(&t.curr.val, 1)
	if atomic.LoadUint64(&t.max.val) < newVal {
		atomic.StoreUint64(&t.max.val, newVal)
	}

	// notify client connected
	nm, _ := message.NewMessage(message.ProtocolV311, message.PUBLISH)
	notifyMsg, _ := nm.(*message.PublishMessage)
	notifyMsg.SetQoS(message.QoS0)
	notifyMsg.SetRetain(false)
	notifyMsg.SetTopic(t.topic + id + "/connected")

	if out, err := json.Marshal(&status); err != nil {
		// todo: put reliable message
		notifyMsg.SetPayload([]byte("data error"))
	} else {
		notifyMsg.SetPayload(out)
	}

	if t.topicsManager != nil {
		t.topicsManager.Publish(notifyMsg)
		t.topicsManager.Retain(notifyMsg)
	}

	// notify remove previous disconnect if any
	nm, _ = message.NewMessage(message.ProtocolV311, message.PUBLISH)
	notifyMsg, _ = nm.(*message.PublishMessage)
	notifyMsg.SetQoS(message.QoS0)
	notifyMsg.SetRetain(false)
	notifyMsg.SetTopic(t.topic + id + "/disconnected")

	if t.topicsManager != nil {
		t.topicsManager.Retain(notifyMsg)
	}
}

// Disconnected remove client from statistic
func (t *clients) Disconnected(id string, reason message.ReasonCode, retain bool) {
	atomic.AddUint64(&t.curr.val, ^uint64(0))

	nm, _ := message.NewMessage(message.ProtocolV311, message.PUBLISH)
	notifyMsg, _ := nm.(*message.PublishMessage)
	notifyMsg.SetQoS(message.QoS0)
	notifyMsg.SetRetain(false)
	notifyMsg.SetTopic(t.topic + id + "/disconnected")
	notifyPayload := clientDisconnectStatus{
		Reason:    "normal",
		Timestamp: time.Now().Format(time.RFC3339),
	}

	if out, err := json.Marshal(&notifyPayload); err != nil {
		notifyMsg.SetPayload([]byte("data error"))
	} else {
		notifyMsg.SetPayload(out)
	}

	if t.topicsManager != nil {
		if retain {
			t.topicsManager.Retain(notifyMsg)
		}
		t.topicsManager.Publish(notifyMsg)
	}

	// remove connected retained message
	nm, _ = message.NewMessage(message.ProtocolV311, message.PUBLISH)
	notifyMsg, _ = nm.(*message.PublishMessage)
	notifyMsg.SetQoS(message.QoS0)
	notifyMsg.SetRetain(false)
	notifyMsg.SetTopic(t.topic + id + "/connected")

	if t.topicsManager != nil {
		t.topicsManager.Retain(notifyMsg)
	}
}
