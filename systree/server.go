package systree

import (
	"encoding/json"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/volantmq/types"
)

type server struct {
	version      string
	upTime       *dynamicValueUpTime
	currTime     *dynamicValueCurrentTime
	capabilities struct {
		SupportedVersions             []mqttp.ProtocolVersion
		MaxQoS                        string
		MaxConnections                uint64
		MaximumPacketSize             uint32
		ServerKeepAlive               uint16
		ReceiveMaximum                uint16
		RetainAvailable               bool
		WildcardSubscriptionAvailable bool
		SubscriptionIDAvailable       bool
		SharedSubscriptionAvailable   bool
	}
}

func newServer(topicPrefix string, dynRetains, staticRetains *[]types.RetainObject) server {
	b := server{
		upTime:   newDynamicValueUpTime(topicPrefix + "/uptime"),
		currTime: newDynamicValueCurrentTime(topicPrefix + "/datetime"),
		version:  "1.0.0",
	}

	m, _ := mqttp.New(mqttp.ProtocolV311, mqttp.PUBLISH)
	msg, _ := m.(*mqttp.Publish)
	msg.SetQoS(mqttp.QoS0)                 // nolint: errcheck
	msg.SetTopic(topicPrefix + "/version") // nolint: errcheck
	msg.SetPayload([]byte(b.version))

	*dynRetains = append(*dynRetains, b.upTime)
	*dynRetains = append(*dynRetains, b.currTime)
	*staticRetains = append(*staticRetains, msg)

	m, _ = mqttp.New(mqttp.ProtocolV311, mqttp.PUBLISH)
	msg, _ = m.(*mqttp.Publish)
	msg.SetQoS(mqttp.QoS0)                      // nolint: errcheck
	msg.SetTopic(topicPrefix + "/capabilities") // nolint: errcheck

	if data, err := json.Marshal(&b.capabilities); err == nil {
		msg.SetPayload(data)
	} else {
		msg.SetPayload([]byte(err.Error()))
	}

	*staticRetains = append(*staticRetains, msg)

	return b
}
