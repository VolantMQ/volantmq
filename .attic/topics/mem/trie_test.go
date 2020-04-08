package mem

import (
	"testing"

	"github.com/VolantMQ/vlapi/mqttp"
	"github.com/VolantMQ/vlapi/vlsubscriber"
	"github.com/VolantMQ/vlapi/vltypes"
	"github.com/stretchr/testify/require"

	"github.com/VolantMQ/volantmq/metrics"
	"github.com/VolantMQ/volantmq/subscriber"

	topicstypes "github.com/VolantMQ/volantmq/topics/types"
)

var config *topicstypes.MemConfig
var retainedSystree []vltypes.RetainObject

func init() {
	metric := metrics.New()

	config = topicstypes.NewMemConfig()
	config.MetricsSubs = metric.Subs()
	config.MetricsPackets = metric.Packets()
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
	sub := &subscriber.Type{}

	req := topicstypes.SubscribeReq{
		Filter: "sport/tennis/player1/#",
		S:      sub,
		Params: vlsubscriber.SubscriptionParams{
			Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
		},
	}

	resp := prov.Subscribe(req)
	require.NotNil(t, resp)
	require.NoError(t, resp.Err)

	subs := publishes{}

	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subs)
	require.Equal(t, 1, len(subs))
}

func TestMatch2(t *testing.T) {
	prov := allocProvider(t)

	sub := &subscriber.Type{}

	req := topicstypes.SubscribeReq{
		Filter: "sport/tennis/player1/#",
		S:      sub,
		Params: vlsubscriber.SubscriptionParams{
			Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
		},
	}

	resp := prov.Subscribe(req)
	require.NotNil(t, resp)
	require.NoError(t, resp.Err)

	subs := publishes{}

	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subs)
	require.Equal(t, 1, len(subs))
}

func TestSNodeMatch3(t *testing.T) {
	prov := allocProvider(t)

	sub := &subscriber.Type{}

	req := topicstypes.SubscribeReq{
		Filter: "sport/tennis/#",
		S:      sub,
		Params: vlsubscriber.SubscriptionParams{
			Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
		},
	}

	resp := prov.Subscribe(req)
	require.NotNil(t, resp)
	require.NoError(t, resp.Err)

	subs := publishes{}
	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subs)
	require.Equal(t, 1, len(subs))
}

func TestMatch4(t *testing.T) {
	prov := allocProvider(t)
	sub := &subscriber.Type{}

	req := topicstypes.SubscribeReq{
		Filter: "#",
		S:      sub,
		Params: vlsubscriber.SubscriptionParams{
			Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
		},
	}

	resp := prov.Subscribe(req)
	require.NotNil(t, resp)
	require.NoError(t, resp.Err)

	subs := publishes{}

	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subs)
	require.Equal(t, 1, len(subs), "should return subscribers")

	subs = publishes{}
	prov.subscriptionSearch("/sport/tennis/player1/anzel", 0, &subs)
	require.Equal(t, 0, len(subs), "should not return subscribers")

	err := prov.subscriptionRemove("#", sub)
	require.NoError(t, err)

	subs = publishes{}
	prov.subscriptionSearch("#", 0, &subs)
	require.Equal(t, 0, len(subs), "should not return subscribers")

	prov.subscriptionInsert("/#", sub, req.Params)

	subs = publishes{}
	prov.subscriptionSearch("bla", 0, &subs)
	require.Equal(t, 0, len(subs), "should not return subscribers")

	subs = publishes{}
	prov.subscriptionSearch("/bla", 0, &subs)
	require.Equal(t, 1, len(subs), "should return subscribers")

	err = prov.subscriptionRemove("/#", sub)
	require.NoError(t, err)

	prov.subscriptionInsert("bla/bla/#", sub, req.Params)

	subs = publishes{}
	prov.subscriptionSearch("bla", 0, &subs)
	require.Equal(t, 0, len(subs), "should not return subscribers")

	subs = publishes{}
	prov.subscriptionSearch("bla/bla", 0, &subs)
	require.Equal(t, 1, len(subs), "should return subscribers")

	subs = publishes{}
	prov.subscriptionSearch("bla/bla/bla", 0, &subs)
	require.Equal(t, 1, len(subs), "should return subscribers")

	subs = publishes{}
	prov.subscriptionSearch("bla/bla/bla/bla", 0, &subs)
	require.Equal(t, 1, len(subs), "should return subscribers")
}

func TestMatch5(t *testing.T) {
	prov := allocProvider(t)
	sub1 := &subscriber.Type{}
	sub2 := &subscriber.Type{}

	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
	}

	prov.subscriptionInsert("sport/tennis/+/+/#", sub1, p)
	prov.subscriptionInsert("sport/tennis/player1/anzel", sub2, p)

	subs := publishes{}
	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subs)

	require.Equal(t, 2, len(subs))
}

func TestMatch6(t *testing.T) {
	prov := allocProvider(t)
	sub1 := &subscriber.Type{}
	sub2 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
	}

	prov.subscriptionInsert("sport/tennis/+/+/+/+/#", sub1, p)
	prov.subscriptionInsert("sport/tennis/player1/anzel", sub2, p)

	subs := publishes{}
	prov.subscriptionSearch("sport/tennis/player1/anzel/bla/bla", 0, &subs)
	require.Equal(t, 1, len(subs))
}

func TestMatch7(t *testing.T) {
	prov := allocProvider(t)

	sub1 := &subscriber.Type{}
	sub2 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
	}

	prov.subscriptionInsert("sport/tennis/#", sub1, p)

	p.Ops = mqttp.SubscriptionOptions(mqttp.QoS1)

	prov.subscriptionInsert("sport/tennis", sub2, p)

	subs := publishes{}
	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subs)
	require.Equal(t, 1, len(subs))
	require.Equal(t, sub1, subs[sub1.Hash()][0].s)
}

func TestMatch8(t *testing.T) {
	prov := allocProvider(t)

	sub := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
	}

	prov.subscriptionInsert("+/+", sub, p)

	subs := publishes{}

	prov.subscriptionSearch("/finance", 0, &subs)
	require.Equal(t, 1, len(subs))
}

func TestMatch9(t *testing.T) {
	prov := allocProvider(t)

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
	}

	prov.subscriptionInsert("/+", sub1, p)

	subs := publishes{}

	prov.subscriptionSearch("/finance", 0, &subs)
	require.Equal(t, 1, len(subs))
}

func TestMatch10(t *testing.T) {
	prov := allocProvider(t)

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
	}

	prov.subscriptionInsert("+", sub1, p)

	subs := publishes{}

	prov.subscriptionSearch("/finance", 0, &subs)
	require.Equal(t, 0, len(subs))
}

func TestInsertRemove(t *testing.T) {
	prov := allocProvider(t)
	sub := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
	}

	prov.subscriptionInsert("#", sub, p)

	subs := publishes{}
	prov.subscriptionSearch("bla", 0, &subs)
	require.Equal(t, 1, len(subs))

	subs = publishes{}
	prov.subscriptionSearch("/bla", 0, &subs)
	require.Equal(t, 0, len(subs))

	err := prov.subscriptionRemove("#", sub)
	require.NoError(t, err)

	subs = publishes{}
	prov.subscriptionSearch("#", 0, &subs)
	require.Equal(t, 0, len(subs))

	prov.subscriptionInsert("/#", sub, p)

	subs = publishes{}
	prov.subscriptionSearch("bla", 0, &subs)
	require.Equal(t, 0, len(subs))

	subs = publishes{}
	prov.subscriptionSearch("/bla", 0, &subs)
	require.Equal(t, 1, len(subs))

	err = prov.subscriptionRemove("#", sub)
	require.EqualError(t, err, topicstypes.ErrNotFound.Error())

	err = prov.subscriptionRemove("/#", sub)
	require.NoError(t, err)
}

func TestInsert1(t *testing.T) {
	prov := allocProvider(t)
	topic := "sport/tennis/player1/#"

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
	}

	prov.subscriptionInsert(topic, sub1, p)
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

	var e *topicSubscriber

	e, ok = level5.subs[sub1.Hash()]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.s)
}

func TestSNodeInsert2(t *testing.T) {
	prov := allocProvider(t)
	topic := "#"

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
	}

	prov.subscriptionInsert(topic, sub1, p)
	require.Equal(t, 1, len(prov.root.children))
	require.Equal(t, 0, len(prov.root.subs))

	n2, ok := prov.root.children[topic]

	require.True(t, ok)
	require.Equal(t, 0, len(n2.children))
	require.Equal(t, 1, len(n2.subs))

	var e *topicSubscriber

	e, ok = n2.subs[sub1.Hash()]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.s)
}

func TestSNodeInsert3(t *testing.T) {
	prov := allocProvider(t)
	topic := "+/tennis/#"

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
	}

	prov.subscriptionInsert(topic, sub1, p)
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

	var e *topicSubscriber

	e, ok = n4.subs[sub1.Hash()]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.s)
}

func TestSNodeInsert4(t *testing.T) {
	prov := allocProvider(t)
	topic := "/finance"

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
	}

	prov.subscriptionInsert(topic, sub1, p)
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

	var e *topicSubscriber

	e, ok = n3.subs[sub1.Hash()]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.s)
}

func TestSNodeInsertDup(t *testing.T) {
	prov := allocProvider(t)
	topic := "/finance"

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
	}

	prov.subscriptionInsert(topic, sub1, p)
	prov.subscriptionInsert(topic, sub1, p)

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

	var e *topicSubscriber

	e, ok = n3.subs[sub1.Hash()]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.s)
}

func TestSNodeRemove1(t *testing.T) {
	prov := allocProvider(t)
	topic := "sport/tennis/player1/#"

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
	}

	prov.subscriptionInsert(topic, sub1, p)

	err := prov.subscriptionRemove(topic, sub1)
	require.NoError(t, err)

	require.Equal(t, 0, len(prov.root.children))
	require.Equal(t, 0, len(prov.root.subs))
}

func TestSNodeRemove2(t *testing.T) {
	prov := allocProvider(t)
	topic := "sport/tennis/player1/#"

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
	}

	prov.subscriptionInsert(topic, sub1, p)

	err := prov.subscriptionRemove("sport/tennis/player1", sub1)
	require.EqualError(t, err, topicstypes.ErrNotFound.Error())
}

func TestSNodeRemove3(t *testing.T) {
	prov := allocProvider(t)
	topic := "sport/tennis/player1/#"

	sub1 := &subscriber.Type{}
	sub2 := &subscriber.Type{}

	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
	}

	prov.subscriptionInsert(topic, sub1, p)
	prov.subscriptionInsert(topic, sub2, p)

	err := prov.subscriptionRemove("sport/tennis/player1/#", nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(prov.root.children))
	require.Equal(t, 0, len(prov.root.subs))
}

func TestRetain1(t *testing.T) {
	prov := allocProvider(t)
	sub := &subscriber.Type{}

	for _, m := range retainedSystree {
		prov.retain(m)
	}

	req := topicstypes.SubscribeReq{
		Filter: "#",
		S:      sub,
		Params: vlsubscriber.SubscriptionParams{
			Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
		},
	}

	resp := prov.Subscribe(req)
	require.NotNil(t, resp)
	require.NoError(t, resp.Err)
	require.Equal(t, 0, len(resp.Retained))

	req.Filter = "$SYS/#"
	resp = prov.Subscribe(req)
	require.NotNil(t, resp)
	require.NoError(t, resp.Err)
	require.Equal(t, len(retainedSystree), len(resp.Retained))
}

func TestRetain2(t *testing.T) {
	prov := allocProvider(t)
	sub := &subscriber.Type{}

	for _, m := range retainedSystree {
		prov.retain(m)
	}

	msg := newPublishMessageLarge("sport/tennis/player1/ricardo", mqttp.QoS1)
	prov.retain(msg)

	req := topicstypes.SubscribeReq{
		Filter: "#",
		S:      sub,
		Params: vlsubscriber.SubscriptionParams{
			Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
		},
	}

	resp := prov.Subscribe(req)
	require.NotNil(t, resp)
	require.NoError(t, resp.Err)

	var rMsg []*mqttp.Publish
	prov.retainSearch("#", &rMsg)
	require.Equal(t, 1, len(rMsg))

	req.Filter = "$SYS/#"
	resp = prov.Subscribe(req)
	require.NotNil(t, resp)
	require.NoError(t, resp.Err)
	require.Equal(t, len(retainedSystree), len(resp.Retained))
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

	var rMsg *mqttp.Publish
	rMsg, ok = n5.retained.(*mqttp.Publish)
	require.True(t, ok)
	require.Equal(t, msg.QoS(), rMsg.QoS())
	require.Equal(t, msg.Topic(), rMsg.Topic())
	require.Equal(t, msg.Payload(), rMsg.Payload())

	// --- Insert msg2

	msg2 := newPublishMessageLarge("sport/tennis/player1/andre", mqttp.QoS1)

	prov.retain(msg2)
	require.Equal(t, 2, len(n4.children))

	n6, ok := n4.children["andre"]

	require.True(t, ok)
	require.Equal(t, 0, len(n6.children))
	require.NotNil(t, n6.retained)

	rMsg, ok = n6.retained.(*mqttp.Publish)
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

	msg1 := newPublishMessageLarge("sport/tennis/ricardo/stats", mqttp.QoS1)
	prov.retain(msg1)

	msg2 := newPublishMessageLarge("sport/tennis/andre/stats", mqttp.QoS1)
	prov.retain(msg2)
	msg3 := newPublishMessageLarge("sport/tennis/andre/bio", mqttp.QoS1)
	prov.retain(msg3)

	var msglist []*mqttp.Publish

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

// nolint:unparam
func newPublishMessageLarge(topic string, qos mqttp.QosType) *mqttp.Publish {
	m, _ := mqttp.New(mqttp.ProtocolV311, mqttp.PUBLISH)

	msg := m.(*mqttp.Publish)

	msg.SetPayload(make([]byte, 1024))
	_ = msg.SetTopic(topic)
	_ = msg.SetQoS(qos)

	return msg
}
