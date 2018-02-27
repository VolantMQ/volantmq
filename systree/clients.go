package systree

import (
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/VolantMQ/mqttp"
	"github.com/VolantMQ/volantmq/types"
)

// ClientConnectStatus is argument to client connected state
type ClientConnectStatus struct {
	Address           string
	Username          string
	Timestamp         string
	ReceiveMaximum    uint32
	MaximumPacketSize uint32
	KeepAlive         uint16
	GeneratedID       bool
	CleanSession      bool
	Durable           bool
	SessionPresent    bool
	PreserveOrder     bool
	MaximumQoS        packet.QosType
	Protocol          packet.ProtocolVersion
	ConnAckCode       packet.ReasonCode
}

type clientDisconnectStatus struct {
	Reason    string
	Timestamp string
}

type clients struct {
	stat
	topicsManager types.TopicMessenger
	topic         string
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
	nm, _ := packet.New(packet.ProtocolV311, packet.PUBLISH)
	notifyMsg, _ := nm.(*packet.Publish)
	notifyMsg.SetRetain(false)
	notifyMsg.SetQoS(packet.QoS0)                   // nolint: errcheck
	notifyMsg.SetTopic(t.topic + id + "/connected") // nolint: errcheck

	if out, err := json.Marshal(&status); err != nil {
		// todo: put reliable message
		notifyMsg.SetPayload([]byte("data error"))
	} else {
		notifyMsg.SetPayload(out)
	}

	t.topicsManager.Publish(notifyMsg) // nolint: errcheck
	t.topicsManager.Retain(notifyMsg)  // nolint: errcheck

	// notify remove previous disconnect if any
	nm, _ = packet.New(packet.ProtocolV311, packet.PUBLISH)
	notifyMsg, _ = nm.(*packet.Publish)
	notifyMsg.SetRetain(false)
	notifyMsg.SetQoS(packet.QoS0)                      // nolint: errcheck
	notifyMsg.SetTopic(t.topic + id + "/disconnected") // nolint: errcheck
	t.topicsManager.Retain(notifyMsg)                  // nolint: errcheck
}

// Disconnected remove client from statistic
func (t *clients) Disconnected(id string, reason packet.ReasonCode) {
	atomic.AddUint64(&t.curr.val, ^uint64(0))

	nm, _ := packet.New(packet.ProtocolV311, packet.PUBLISH)
	notifyMsg, _ := nm.(*packet.Publish)
	notifyMsg.SetRetain(false)
	notifyMsg.SetQoS(packet.QoS0)                      // nolint: errcheck
	notifyMsg.SetTopic(t.topic + id + "/disconnected") // nolint: errcheck
	notifyPayload := clientDisconnectStatus{
		Reason:    "normal",
		Timestamp: time.Now().Format(time.RFC3339),
	}

	if out, err := json.Marshal(&notifyPayload); err != nil {
		notifyMsg.SetPayload([]byte("data error"))
	} else {
		notifyMsg.SetPayload(out)
	}

	t.topicsManager.Publish(notifyMsg) // nolint: errcheck

	// remove connected retained message
	nm, _ = packet.New(packet.ProtocolV311, packet.PUBLISH)
	notifyMsg, _ = nm.(*packet.Publish)
	notifyMsg.SetRetain(false)
	notifyMsg.SetQoS(packet.QoS0)                   // nolint: errcheck
	notifyMsg.SetTopic(t.topic + id + "/connected") // nolint: errcheck
	t.topicsManager.Retain(notifyMsg)               // nolint: errcheck
}
