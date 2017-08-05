package systree

import (
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/types"
)

type broker struct {
	version  string
	upTime   *dynamicValueUpTime
	currTime *dynamicValueCurrentTime
}

func newBroker(topicPrefix string, dynRetains, staticRetains *[]types.RetainObject) broker {
	b := broker{
		upTime:   newDynamicValueUpTime(topicPrefix + "/uptime"),
		currTime: newDynamicValueCurrentTime(topicPrefix + "/datetime"),
		version:  "1.0.0",
	}

	m, _ := message.NewMessage(message.ProtocolV311, message.PUBLISH)
	msg, _ := m.(*message.PublishMessage)
	msg.SetQoS(message.QoS0)
	msg.SetTopic(topicPrefix + "/version")
	msg.SetPayload([]byte(b.version))

	*dynRetains = append(*dynRetains, b.upTime)
	*dynRetains = append(*dynRetains, b.currTime)
	*staticRetains = append(*staticRetains, msg)

	return b
}
