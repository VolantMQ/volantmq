package mem

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/subscriber"
	"github.com/troian/surgemq/systree"
	"github.com/troian/surgemq/topics/types"
	"github.com/troian/surgemq/types"
)

var sysTree systree.Provider
var config *topicsTypes.MemConfig
var retainedSystree []types.RetainObject

func init() {
	config = topicsTypes.NewMemConfig()
	sysTree, retainedSystree, _, _ = systree.NewTree("$SYS/broker")

	config.Stat = sysTree.Topics()
}

func allocProvider(t *testing.T) *provider {
	prov, err := NewMemProvider(config)
	require.NoError(t, err)

	if p, ok := prov.(*provider); ok {
		return p
	}
	t.Fail()
	return nil
}

func TestMatch1(t *testing.T) {
	prov := allocProvider(t)
	sub := &subscriber.ProviderType{}

	prov.Subscribe("sport/tennis/player1/#", message.QoS1, sub, 0)

	subscribers := publishEntries{}

	prov.subscriptionSearch("sport/tennis/player1/anzel", &subscribers)
	require.Equal(t, 1, len(subscribers))
}

func TestMatch2(t *testing.T) {
	prov := allocProvider(t)

	sub := &subscriber.ProviderType{}

	prov.Subscribe("sport/tennis/player1/#", message.QoS2, sub, 0)

	subscribers := publishEntries{}

	prov.subscriptionSearch("sport/tennis/player1/anzel", &subscribers)
	require.Equal(t, 1, len(subscribers))
}

func TestSNodeMatch3(t *testing.T) {
	prov := allocProvider(t)

	sub := &subscriber.ProviderType{}

	prov.Subscribe("sport/tennis/#", message.QoS2, sub, 0)

	subscribers := publishEntries{}
	prov.subscriptionSearch("sport/tennis/player1/anzel", &subscribers)
	require.Equal(t, 1, len(subscribers))
}

func TestMatch4(t *testing.T) {
	prov := allocProvider(t)
	sub := &subscriber.ProviderType{}

	prov.Subscribe("#", message.QoS2, sub, 0)

	subscribers := publishEntries{}

	prov.subscriptionSearch("sport/tennis/player1/anzel", &subscribers)
	require.Equal(t, 1, len(subscribers), "should return subscribers")

	subscribers = publishEntries{}
	prov.subscriptionSearch("/sport/tennis/player1/anzel", &subscribers)
	require.Equal(t, 0, len(subscribers), "should not return subscribers")

	err := prov.subscriptionRemove("#", sub)
	require.NoError(t, err)

	subscribers = publishEntries{}
	prov.subscriptionSearch("#", &subscribers)
	require.Equal(t, 0, len(subscribers), "should not return subscribers")

	prov.subscriptionInsert("/#", message.QoS2, sub, 0)

	subscribers = publishEntries{}
	prov.subscriptionSearch("bla", &subscribers)
	require.Equal(t, 0, len(subscribers), "should not return subscribers")

	subscribers = publishEntries{}
	prov.subscriptionSearch("/bla", &subscribers)
	require.Equal(t, 1, len(subscribers), "should return subscribers")

	err = prov.subscriptionRemove("/#", sub)
	require.NoError(t, err)

	prov.subscriptionInsert("bla/bla/#", message.QoS2, sub, 0)

	subscribers = publishEntries{}
	prov.subscriptionSearch("bla", &subscribers)
	require.Equal(t, 0, len(subscribers), "should not return subscribers")

	subscribers = publishEntries{}
	prov.subscriptionSearch("bla/bla", &subscribers)
	require.Equal(t, 1, len(subscribers), "should return subscribers")

	subscribers = publishEntries{}
	prov.subscriptionSearch("bla/bla/bla", &subscribers)
	require.Equal(t, 1, len(subscribers), "should return subscribers")

	subscribers = publishEntries{}
	prov.subscriptionSearch("bla/bla/bla/bla", &subscribers)
	require.Equal(t, 1, len(subscribers), "should return subscribers")
}

func TestMatch5(t *testing.T) {
	prov := allocProvider(t)
	sub1 := &subscriber.ProviderType{}
	sub2 := &subscriber.ProviderType{}

	prov.subscriptionInsert("sport/tennis/+/+/#", message.QoS1, sub1, 0)
	prov.subscriptionInsert("sport/tennis/player1/anzel", message.QoS1, sub2, 0)

	subscribers := publishEntries{}
	prov.subscriptionSearch("sport/tennis/player1/anzel", &subscribers)

	require.Equal(t, 2, len(subscribers))
}

func TestMatch6(t *testing.T) {
	prov := allocProvider(t)
	sub1 := &subscriber.ProviderType{}
	sub2 := &subscriber.ProviderType{}

	prov.subscriptionInsert("sport/tennis/+/+/+/+/#", message.QoS1, sub1, 0)
	prov.subscriptionInsert("sport/tennis/player1/anzel", message.QoS1, sub2, 0)

	subscribers := publishEntries{}
	prov.subscriptionSearch("sport/tennis/player1/anzel/bla/bla", &subscribers)
	require.Equal(t, 1, len(subscribers))
}

func TestMatch7(t *testing.T) {
	prov := allocProvider(t)

	sub1 := &subscriber.ProviderType{}
	sub2 := &subscriber.ProviderType{}

	prov.subscriptionInsert("sport/tennis/#", message.QoS2, sub1, 0)

	prov.subscriptionInsert("sport/tennis", message.QoS1, sub2, 0)

	subscribers := publishEntries{}
	prov.subscriptionSearch("sport/tennis/player1/anzel", &subscribers)
	require.Equal(t, 1, len(subscribers))
	require.Equal(t, sub1, subscribers[sub1.Hash()][0].s)
}

func TestMatch8(t *testing.T) {
	prov := allocProvider(t)

	sub1 := &subscriber.ProviderType{}

	prov.subscriptionInsert("+/+", message.QoS2, sub1, 0)

	subscribers := publishEntries{}

	prov.subscriptionSearch("/finance", &subscribers)
	require.Equal(t, 1, len(subscribers))
}

func TestMatch9(t *testing.T) {
	prov := allocProvider(t)

	sub1 := &subscriber.ProviderType{}

	prov.subscriptionInsert("/+", message.QoS2, sub1, 0)

	subscribers := publishEntries{}

	prov.subscriptionSearch("/finance", &subscribers)
	require.Equal(t, 1, len(subscribers))
}

func TestMatch10(t *testing.T) {
	prov := allocProvider(t)

	sub1 := &subscriber.ProviderType{}

	prov.subscriptionInsert("+", message.QoS2, sub1, 0)

	subscribers := publishEntries{}

	prov.subscriptionSearch("/finance", &subscribers)
	require.Equal(t, 0, len(subscribers))
}

func TestInsertRemove(t *testing.T) {
	prov := allocProvider(t)
	sub := &subscriber.ProviderType{}

	prov.subscriptionInsert("#", message.QoS2, sub, 0)

	subscribers := publishEntries{}
	prov.subscriptionSearch("bla", &subscribers)
	require.Equal(t, 1, len(subscribers))

	subscribers = publishEntries{}
	prov.subscriptionSearch("/bla", &subscribers)
	require.Equal(t, 0, len(subscribers))

	err := prov.subscriptionRemove("#", sub)
	require.NoError(t, err)

	subscribers = publishEntries{}
	prov.subscriptionSearch("#", &subscribers)
	require.Equal(t, 0, len(subscribers))

	prov.subscriptionInsert("/#", message.QoS2, sub, 0)

	subscribers = publishEntries{}
	prov.subscriptionSearch("bla", &subscribers)
	require.Equal(t, 0, len(subscribers))

	subscribers = publishEntries{}
	prov.subscriptionSearch("/bla", &subscribers)
	require.Equal(t, 1, len(subscribers))

	err = prov.subscriptionRemove("#", sub)
	require.EqualError(t, err, topicsTypes.ErrNotFound.Error())

	err = prov.subscriptionRemove("/#", sub)
	require.NoError(t, err)
}

func TestInsert1(t *testing.T) {
	prov := allocProvider(t)
	topic := "sport/tennis/player1/#"

	sub1 := &subscriber.ProviderType{}
	prov.subscriptionInsert(topic, message.QoS1, sub1, 0)
	require.Equal(t, 1, len(prov.root.children))
	require.Equal(t, 0, len(prov.root.subs))

	level2, ok := prov.root.children["sport"]
	require.True(t, ok)
	require.Equal(t, 1, len(level2.children))
	require.Equal(t, 0, len(level2.subs))

	level3, ok := level2.children["tennis"]

	require.True(t, ok)
	require.Equal(t, 1, len(level3.children))
	require.Equal(t, 0, len(level3.subs))

	level4, ok := level3.children["player1"]

	require.True(t, ok)
	require.Equal(t, 1, len(level4.children))
	require.Equal(t, 0, len(level4.subs))

	level5, ok := level4.children["#"]

	require.True(t, ok)
	require.Equal(t, 0, len(level5.children))
	require.Equal(t, 1, len(level5.subs))

	var e *subscribedEntry

	e, ok = level5.subs[sub1.Hash()]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.s)
}

func TestSNodeInsert2(t *testing.T) {
	prov := allocProvider(t)
	topic := "#"

	sub1 := &subscriber.ProviderType{}

	prov.subscriptionInsert(topic, message.QoS1, sub1, 0)
	require.Equal(t, 1, len(prov.root.children))
	require.Equal(t, 0, len(prov.root.subs))

	n2, ok := prov.root.children[topic]

	require.True(t, ok)
	require.Equal(t, 0, len(n2.children))
	require.Equal(t, 1, len(n2.subs))

	var e *subscribedEntry

	e, ok = n2.subs[sub1.Hash()]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.s)
}

func TestSNodeInsert3(t *testing.T) {
	prov := allocProvider(t)
	topic := "+/tennis/#"

	sub1 := &subscriber.ProviderType{}

	prov.subscriptionInsert(topic, message.QoS1, sub1, 0)
	require.Equal(t, 1, len(prov.root.children))
	require.Equal(t, 0, len(prov.root.subs))

	n2, ok := prov.root.children["+"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.children))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.children["tennis"]

	require.True(t, ok)
	require.Equal(t, 1, len(n3.children))
	require.Equal(t, 0, len(n3.subs))

	n4, ok := n3.children["#"]

	require.True(t, ok)
	require.Equal(t, 0, len(n4.children))
	require.Equal(t, 1, len(n4.subs))

	var e *subscribedEntry

	e, ok = n4.subs[sub1.Hash()]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.s)
}

func TestSNodeInsert4(t *testing.T) {
	prov := allocProvider(t)
	topic := "/finance"

	sub1 := &subscriber.ProviderType{}

	prov.subscriptionInsert(topic, message.QoS1, sub1, 0)
	require.Equal(t, 1, len(prov.root.children))
	require.Equal(t, 0, len(prov.root.subs))

	n2, ok := prov.root.children[""]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.children))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.children["finance"]

	require.True(t, ok)
	require.Equal(t, 0, len(n3.children))
	require.Equal(t, 1, len(n3.subs))

	var e *subscribedEntry

	e, ok = n3.subs[sub1.Hash()]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.s)
}

func TestSNodeInsertDup(t *testing.T) {
	prov := allocProvider(t)
	topic := "/finance"

	sub1 := &subscriber.ProviderType{}

	prov.subscriptionInsert(topic, message.QoS1, sub1, 0)
	prov.subscriptionInsert(topic, message.QoS1, sub1, 0)

	require.Equal(t, 1, len(prov.root.children))
	require.Equal(t, 0, len(prov.root.subs))

	n2, ok := prov.root.children[""]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.children))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.children["finance"]

	require.True(t, ok)
	require.Equal(t, 0, len(n3.children))
	require.Equal(t, 1, len(n3.subs))

	var e *subscribedEntry

	e, ok = n3.subs[sub1.Hash()]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.s)
}

func TestSNodeRemove1(t *testing.T) {
	prov := allocProvider(t)
	topic := "sport/tennis/player1/#"

	sub1 := &subscriber.ProviderType{}

	prov.subscriptionInsert(topic, message.QoS1, sub1, 0)

	err := prov.subscriptionRemove(topic, sub1)
	require.NoError(t, err)

	require.Equal(t, 0, len(prov.root.children))
	require.Equal(t, 0, len(prov.root.subs))
}

func TestSNodeRemove2(t *testing.T) {
	prov := allocProvider(t)
	topic := "sport/tennis/player1/#"

	sub1 := &subscriber.ProviderType{}

	prov.subscriptionInsert(topic, message.QoS1, sub1, 0)

	err := prov.subscriptionRemove("sport/tennis/player1", sub1)
	require.EqualError(t, topicsTypes.ErrNotFound, err.Error())
}

func TestSNodeRemove3(t *testing.T) {
	prov := allocProvider(t)
	topic := "sport/tennis/player1/#"

	sub1 := &subscriber.ProviderType{}
	sub2 := &subscriber.ProviderType{}

	prov.subscriptionInsert(topic, message.QoS1, sub1, 0)
	prov.subscriptionInsert(topic, message.QoS1, sub2, 0)

	err := prov.subscriptionRemove("sport/tennis/player1/#", nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(prov.root.children))
	require.Equal(t, 0, len(prov.root.subs))
}

func TestRetain1(t *testing.T) {
	prov := allocProvider(t)
	sub := &subscriber.ProviderType{}

	for _, m := range retainedSystree {
		prov.retain(m)
	}

	_, rMsg, _ := prov.Subscribe("#", message.QoS1, sub, 0)
	require.Equal(t, 0, len(rMsg))

	_, rMsg, _ = prov.Subscribe("$SYS", message.QoS1, sub, 0)
	require.Equal(t, 0, len(rMsg))

	_, rMsg, _ = prov.Subscribe("$SYS/#", message.QoS1, sub, 0)
	require.Equal(t, len(retainedSystree), len(rMsg))
}

func TestRetain2(t *testing.T) {
	prov := allocProvider(t)
	sub := &subscriber.ProviderType{}

	for _, m := range retainedSystree {
		prov.retain(m)
	}

	msg := newPublishMessageLarge("sport/tennis/player1/ricardo", message.QoS1)
	prov.retain(msg)

	prov.Subscribe("#", message.QoS1, sub, 0)

	var rMsg []*message.PublishMessage
	prov.retainSearch("#", &rMsg)
	require.Equal(t, 1, len(rMsg))

	_, rMsg, _ = prov.Subscribe("$SYS/#", message.QoS1, sub, 0)
	require.Equal(t, len(retainedSystree), len(rMsg))
}

func TestRNodeInsertRemove(t *testing.T) {
	prov := allocProvider(t)

	// --- Insert msg1

	msg := newPublishMessageLarge("sport/tennis/player1/ricardo", 1)

	n := prov.root
	prov.retain(msg)
	require.Equal(t, 1, len(n.children))
	require.Nil(t, n.retained)

	n2, ok := n.children["sport"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.children))
	require.Nil(t, n2.retained)

	n3, ok := n2.children["tennis"]

	require.True(t, ok)
	require.Equal(t, 1, len(n3.children))
	require.Nil(t, n3.retained)

	n4, ok := n3.children["player1"]

	require.True(t, ok)
	require.Equal(t, 1, len(n4.children))
	require.Nil(t, n4.retained)

	n5, ok := n4.children["ricardo"]

	require.True(t, ok)
	require.Equal(t, 0, len(n5.children))
	require.NotNil(t, n5.retained)

	var rMsg *message.PublishMessage
	rMsg, ok = n5.retained.(*message.PublishMessage)
	require.True(t, ok)
	require.Equal(t, msg.QoS(), rMsg.QoS())
	require.Equal(t, msg.Topic(), rMsg.Topic())
	require.Equal(t, msg.Payload(), rMsg.Payload())

	// --- Insert msg2

	msg2 := newPublishMessageLarge("sport/tennis/player1/andre", message.QoS1)

	prov.retain(msg2)
	require.Equal(t, 2, len(n4.children))

	n6, ok := n4.children["andre"]

	require.True(t, ok)
	require.Equal(t, 0, len(n6.children))
	require.NotNil(t, n6.retained)

	rMsg, ok = n6.retained.(*message.PublishMessage)
	require.True(t, ok)
	require.Equal(t, msg2.QoS(), rMsg.QoS())
	require.Equal(t, msg2.Topic(), rMsg.Topic())

	// --- Remove

	msg2.SetPayload([]byte{})
	err := prov.retainRemove("sport/tennis/player1/andre")
	require.NoError(t, err)
	require.Equal(t, 1, len(n4.children))
}

func TestRNodeMatch(t *testing.T) {
	prov := allocProvider(t)

	msg1 := newPublishMessageLarge("sport/tennis/ricardo/stats", message.QoS1)
	prov.retain(msg1)

	msg2 := newPublishMessageLarge("sport/tennis/andre/stats", message.QoS1)
	prov.retain(msg2)
	msg3 := newPublishMessageLarge("sport/tennis/andre/bio", message.QoS1)
	prov.retain(msg3)

	var msglist []*message.PublishMessage

	// ---

	msglist, _ = prov.Retained(msg1.Topic())
	require.Equal(t, 1, len(msglist))

	// ---
	msglist, _ = prov.Retained(msg2.Topic())
	require.Equal(t, 1, len(msglist))

	// ---
	msglist, _ = prov.Retained(msg3.Topic())
	require.Equal(t, 1, len(msglist))

	// ---
	msglist, _ = prov.Retained("sport/tennis/andre/+")
	require.Equal(t, 2, len(msglist))

	// ---
	msglist, _ = prov.Retained("sport/tennis/andre/#")
	require.Equal(t, 2, len(msglist))

	// ---
	msglist, _ = prov.Retained("sport/tennis/+/stats")
	require.Equal(t, 2, len(msglist))

	// ---
	msglist, _ = prov.Retained("sport/tennis/#")
	require.Equal(t, 3, len(msglist))
}

func newPublishMessageLarge(topic string, qos message.QosType) *message.PublishMessage {
	m, _ := message.NewMessage(message.ProtocolV311, message.PUBLISH)

	msg := m.(*message.PublishMessage)

	msg.SetPayload(make([]byte, 1024))
	msg.SetTopic(topic) // nolint: errcheck
	msg.SetQoS(qos)     // nolint: errcheck

	return msg
}
