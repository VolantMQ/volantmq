package memlockfree

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
	require.NoError(t, resp.Err)

	subscribers := publishes{}

	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subscribers)
	require.Equal(t, 1, len(subscribers))
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
	require.NoError(t, resp.Err)

	subscribers := publishes{}

	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subscribers)
	require.Equal(t, 1, len(subscribers))
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
	require.NoError(t, resp.Err)

	subscribers := publishes{}
	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subscribers)
	require.Equal(t, 1, len(subscribers))
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
	require.NoError(t, resp.Err)

	subscribers := publishes{}

	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subscribers)
	require.Equal(t, 1, len(subscribers), "should return subscribers")

	subscribers = publishes{}
	prov.subscriptionSearch("/sport/tennis/player1/anzel", 0, &subscribers)
	require.Equal(t, 0, len(subscribers), "should not return subscribers")

	err := prov.subscriptionRemove("#", sub)
	require.NoError(t, err)

	subscribers = publishes{}
	prov.subscriptionSearch("#", 0, &subscribers)
	require.Equal(t, 0, len(subscribers), "should not return subscribers")

	prov.subscriptionInsert("/#", sub, req.Params)

	subscribers = publishes{}
	prov.subscriptionSearch("bla", 0, &subscribers)
	require.Equal(t, 0, len(subscribers), "should not return subscribers")

	subscribers = publishes{}
	prov.subscriptionSearch("/bla", 0, &subscribers)
	require.Equal(t, 1, len(subscribers), "should return subscribers")

	err = prov.subscriptionRemove("/#", sub)
	require.NoError(t, err)

	prov.subscriptionInsert("bla/bla/#", sub, req.Params)

	subscribers = publishes{}
	prov.subscriptionSearch("bla", 0, &subscribers)
	require.Equal(t, 0, len(subscribers), "should not return subscribers")

	subscribers = publishes{}
	prov.subscriptionSearch("bla/bla", 0, &subscribers)
	require.Equal(t, 1, len(subscribers), "should return subscribers")

	subscribers = publishes{}
	prov.subscriptionSearch("bla/bla/bla", 0, &subscribers)
	require.Equal(t, 1, len(subscribers), "should return subscribers")

	subscribers = publishes{}
	prov.subscriptionSearch("bla/bla/bla/bla", 0, &subscribers)
	require.Equal(t, 1, len(subscribers), "should return subscribers")
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

	subscribers := publishes{}
	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subscribers)

	require.Equal(t, 2, len(subscribers))
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

	subscribers := publishes{}
	prov.subscriptionSearch("sport/tennis/player1/anzel/bla/bla", 0, &subscribers)
	require.Equal(t, 1, len(subscribers))
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

	subscribers := publishes{}
	prov.subscriptionSearch("sport/tennis/player1/anzel", 0, &subscribers)
	require.Equal(t, 1, len(subscribers))
	require.Equal(t, sub1, subscribers[sub1.Hash()][0].s)
}

func TestMatch8(t *testing.T) {
	prov := allocProvider(t)

	sub := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
	}

	prov.subscriptionInsert("+/+", sub, p)

	subscribers := publishes{}

	prov.subscriptionSearch("/finance", 0, &subscribers)
	require.Equal(t, 1, len(subscribers))
}

func TestMatch9(t *testing.T) {
	prov := allocProvider(t)

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
	}

	prov.subscriptionInsert("/+", sub1, p)

	subscribers := publishes{}

	prov.subscriptionSearch("/finance", 0, &subscribers)
	require.Equal(t, 1, len(subscribers))
}

func TestMatch10(t *testing.T) {
	prov := allocProvider(t)

	sub1 := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
	}

	prov.subscriptionInsert("+", sub1, p)

	subscribers := publishes{}

	prov.subscriptionSearch("/finance", 0, &subscribers)
	require.Equal(t, 0, len(subscribers))
}

func TestInsertRemove(t *testing.T) {
	prov := allocProvider(t)
	sub := &subscriber.Type{}
	p := vlsubscriber.SubscriptionParams{
		Ops: mqttp.SubscriptionOptions(mqttp.QoS2),
	}

	prov.subscriptionInsert("#", sub, p)

	subscribers := publishes{}
	prov.subscriptionSearch("bla", 0, &subscribers)
	require.Equal(t, 1, len(subscribers))

	subscribers = publishes{}
	prov.subscriptionSearch("/bla", 0, &subscribers)
	require.Equal(t, 0, len(subscribers))

	err := prov.subscriptionRemove("#", sub)
	require.NoError(t, err)

	subscribers = publishes{}
	prov.subscriptionSearch("#", 0, &subscribers)
	require.Equal(t, 0, len(subscribers))

	prov.subscriptionInsert("/#", sub, p)

	subscribers = publishes{}
	prov.subscriptionSearch("bla", 0, &subscribers)
	require.Equal(t, 0, len(subscribers))

	subscribers = publishes{}
	prov.subscriptionSearch("/bla", 0, &subscribers)
	require.Equal(t, 1, len(subscribers))

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
	require.Equal(t, int32(1), prov.root.kidsCount)
	require.Equal(t, int32(0), prov.root.subsCount)

	nd, ok := prov.root.children.Load("sport")
	require.True(t, ok)
	level2 := nd.(*node)

	require.Equal(t, int32(1), level2.kidsCount)
	require.Equal(t, int32(0), level2.subsCount)

	nd, ok = level2.children.Load("tennis")
	require.True(t, ok)
	level3 := nd.(*node)

	require.Equal(t, int32(1), level3.kidsCount)
	require.Equal(t, int32(0), level3.subsCount)

	nd, ok = level3.children.Load("player1")
	require.True(t, ok)
	level4 := nd.(*node)

	require.Equal(t, int32(1), level4.kidsCount)
	require.Equal(t, int32(0), level4.subsCount)

	nd, ok = level4.children.Load("#")
	require.True(t, ok)
	level5 := nd.(*node)

	require.Equal(t, int32(0), level5.kidsCount)
	require.Equal(t, int32(1), level5.subsCount)

	nd, ok = level5.subs.Load(sub1.Hash())
	require.Equal(t, true, ok)
	e := nd.(*topicSubscriber)

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

	require.Equal(t, int32(1), prov.root.kidsCount)
	require.Equal(t, int32(0), prov.root.subsCount)

	nd, ok := prov.root.children.Load(topic)
	require.True(t, ok)
	n2 := nd.(*node)
	require.Equal(t, int32(0), n2.kidsCount)
	require.Equal(t, int32(1), n2.subsCount)

	nd, ok = n2.subs.Load(sub1.Hash())
	require.Equal(t, true, ok)
	e := nd.(*topicSubscriber)

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

	require.Equal(t, int32(1), prov.root.kidsCount)
	require.Equal(t, int32(0), prov.root.subsCount)

	nd, ok := prov.root.children.Load("+")
	require.True(t, ok)
	n2 := nd.(*node)

	require.Equal(t, int32(1), n2.kidsCount)
	require.Equal(t, int32(0), n2.subsCount)

	nd, ok = n2.children.Load("tennis")
	require.True(t, ok)
	n3 := nd.(*node)

	require.Equal(t, int32(1), n3.kidsCount)
	require.Equal(t, int32(0), n3.subsCount)

	nd, ok = n3.children.Load("#")
	require.True(t, ok)
	n4 := nd.(*node)

	require.Equal(t, int32(0), n4.kidsCount)
	require.Equal(t, int32(1), n4.subsCount)

	nd, ok = n4.subs.Load(sub1.Hash())
	require.Equal(t, true, ok)
	e := nd.(*topicSubscriber)

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

	require.Equal(t, int32(1), prov.root.kidsCount)
	require.Equal(t, int32(0), prov.root.subsCount)

	nd, ok := prov.root.children.Load("")
	require.True(t, ok)
	n2 := nd.(*node)

	require.Equal(t, int32(1), n2.kidsCount)
	require.Equal(t, int32(0), n2.subsCount)

	nd, ok = n2.children.Load("finance")
	require.True(t, ok)
	n3 := nd.(*node)

	require.Equal(t, int32(0), n3.kidsCount)
	require.Equal(t, int32(1), n3.subsCount)

	nd, ok = n3.subs.Load(sub1.Hash())
	require.Equal(t, true, ok)
	e := nd.(*topicSubscriber)

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

	require.Equal(t, int32(1), prov.root.kidsCount)
	require.Equal(t, int32(0), prov.root.subsCount)

	nd, ok := prov.root.children.Load("")
	require.True(t, ok)
	n2 := nd.(*node)
	require.Equal(t, int32(1), n2.kidsCount)
	require.Equal(t, int32(0), n2.subsCount)

	nd, ok = n2.children.Load("finance")
	require.True(t, ok)
	n3 := nd.(*node)
	require.Equal(t, int32(0), n3.kidsCount)
	require.Equal(t, int32(1), n3.subsCount)

	nd, ok = n3.subs.Load(sub1.Hash())
	require.Equal(t, true, ok)
	e := nd.(*topicSubscriber)

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

	require.Equal(t, int32(0), prov.root.subsCount)
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
	require.Equal(t, int32(0), prov.root.subsCount)
}

func TestRetain1(t *testing.T) {
	prov := allocProvider(t)

	for _, m := range retainedSystree {
		prov.retain(m)
	}

	req := &topicstypes.SubscribeReq{
		Filter: "#",
		S:      &subscriber.Type{},
		Params: vlsubscriber.SubscriptionParams{
			Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
		},
	}

	resp := &topicstypes.SubscribeResp{}

	prov.subscribe(req, resp)
	require.NoError(t, resp.Err)
	require.Equal(t, 0, len(resp.Retained))

	req.Filter = "$SYS"
	resp = &topicstypes.SubscribeResp{}
	prov.subscribe(req, resp)
	require.NoError(t, resp.Err)
	require.Equal(t, 0, len(resp.Retained))

	req.Filter = "$SYS/#"
	resp = &topicstypes.SubscribeResp{}
	prov.subscribe(req, resp)
	require.NoError(t, resp.Err)
	require.Equal(t, len(retainedSystree), len(resp.Retained))
}

func TestRetain2(t *testing.T) {
	prov := allocProvider(t)

	for _, m := range retainedSystree {
		prov.retain(m)
	}

	msg := newPublishMessageLarge("sport/tennis/player1/ricardo", mqttp.QoS1)
	prov.retain(msg)

	req := &topicstypes.SubscribeReq{
		Filter: "#",
		S:      &subscriber.Type{},
		Params: vlsubscriber.SubscriptionParams{
			Ops: mqttp.SubscriptionOptions(mqttp.QoS1),
		},
	}

	resp := &topicstypes.SubscribeResp{}
	prov.subscribe(req, resp)
	require.NoError(t, resp.Err)
	require.Equal(t, 1, len(resp.Retained))

	req.Filter = "$SYS/#"
	resp = &topicstypes.SubscribeResp{}
	prov.subscribe(req, resp)
	require.NoError(t, resp.Err)
	require.Equal(t, len(retainedSystree), len(resp.Retained))
}

func TestRNodeInsertRemove(t *testing.T) {
	prov := allocProvider(t)

	// --- Insert msg1

	msg := newPublishMessageLarge("sport/tennis/player1/ricardo", 1)

	n := prov.root
	prov.retain(msg)
	require.Equal(t, int32(1), n.kidsCount)
	require.Nil(t, n.retained.Load().(retainer).val)

	nd, ok := n.children.Load("sport")
	require.True(t, ok)
	n2 := nd.(*node)

	require.Equal(t, int32(1), n2.kidsCount)
	require.Nil(t, n2.retained.Load().(retainer).val)

	nd, ok = n2.children.Load("tennis")
	require.True(t, ok)
	n3 := nd.(*node)

	require.Equal(t, int32(1), n3.kidsCount)
	require.Nil(t, n3.retained.Load().(retainer).val)

	nd, ok = n3.children.Load("player1")
	require.True(t, ok)
	n4 := nd.(*node)
	require.Equal(t, int32(1), n4.kidsCount)
	require.Nil(t, n4.retained.Load().(retainer).val)

	nd, ok = n4.children.Load("ricardo")
	require.True(t, ok)
	n5 := nd.(*node)

	require.Equal(t, int32(0), n5.kidsCount)
	require.NotNil(t, n5.retained.Load().(retainer).val)

	imsg := n5.retained.Load().(retainer).val
	var rMsg *mqttp.Publish
	rMsg, ok = imsg.(*mqttp.Publish)
	require.True(t, ok)

	require.Equal(t, msg.QoS(), rMsg.QoS())
	require.Equal(t, msg.Topic(), rMsg.Topic())
	require.Equal(t, msg.Payload(), rMsg.Payload())

	// --- Insert msg2

	msg2 := newPublishMessageLarge("sport/tennis/player1/andre", mqttp.QoS1)

	prov.retain(msg2)
	require.Equal(t, int32(2), n4.kidsCount)

	nd, ok = n4.children.Load("andre")
	require.True(t, ok)
	n6 := nd.(*node)
	require.Equal(t, int32(0), n6.kidsCount)
	require.NotNil(t, n6.retained.Load().(retainer).val)

	imsg = n6.retained.Load().(retainer).val
	rMsg, ok = imsg.(*mqttp.Publish)
	require.True(t, ok)
	require.Equal(t, msg2.QoS(), rMsg.QoS())
	require.Equal(t, msg2.Topic(), rMsg.Topic())

	// --- Remove

	msg2.SetPayload([]byte{})
	err := prov.retainRemove("sport/tennis/player1/andre")
	require.NoError(t, err)
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

// nolint unparam
func newPublishMessageLarge(topic string, qos mqttp.QosType) *mqttp.Publish {
	m, _ := mqttp.New(mqttp.ProtocolV311, mqttp.PUBLISH)

	msg := m.(*mqttp.Publish)

	msg.SetPayload(make([]byte, 1024))
	_ = msg.SetTopic(topic)
	_ = msg.SetQoS(qos)

	return msg
}
