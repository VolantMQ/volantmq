package mem

import (
	"testing"

	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/troian/surgemq/message"
	"github.com/troian/surgemq/types"
)

func TestNextTopicLevelSuccess(t *testing.T) {
	topics := [][]byte{
		[]byte("sport/tennis/player1/#"),
		[]byte("sport/tennis/player1/ranking"),
		[]byte("sport/#"),
		[]byte("#"),
		[]byte("sport/tennis/#"),
		[]byte("+"),
		[]byte("+/tennis/#"),
		[]byte("sport/+/player1"),
		[]byte("/finance"),
	}

	levels := [][][]byte{
		{[]byte("sport"), []byte("tennis"), []byte("player1"), []byte("#")},
		{[]byte("sport"), []byte("tennis"), []byte("player1"), []byte("ranking")},
		{[]byte("sport"), []byte("#")},
		{[]byte("#")},
		{[]byte("sport"), []byte("tennis"), []byte("#")},
		{[]byte("+")},
		{[]byte("+"), []byte("tennis"), []byte("#")},
		{[]byte("sport"), []byte("+"), []byte("player1")},
		{[]byte("+"), []byte("finance")},
	}

	for i, topic := range topics {
		var tl string
		var rem = string(topic)
		var err error

		for _, level := range levels[i] {
			tl, rem, err = nextTopicLevel(string(rem))
			require.NoError(t, err)
			require.Equal(t, string(level), tl)
		}
	}
}

func TestNextTopicLevelFailure(t *testing.T) {
	topics := [][]byte{
		[]byte("sport/tennis#"),
		[]byte("sport/tennis/#/ranking"),
		[]byte("sport+"),
	}

	var rem string
	var err error

	_, rem, err = nextTopicLevel(string(topics[0]))
	require.NoError(t, err)

	_, _, err = nextTopicLevel(rem)
	require.Error(t, err)

	_, rem, err = nextTopicLevel(string(topics[1]))
	require.NoError(t, err)

	_, rem, err = nextTopicLevel(rem)
	require.NoError(t, err)

	_, _, err = nextTopicLevel(rem)
	require.Error(t, err)

	_, _, err = nextTopicLevel(string(topics[2]))
	require.Error(t, err)
}

func TestSNodeInsert1(t *testing.T) {
	n := newSNode()
	topic := "sport/tennis/player1/#"

	sub1 := &types.Subscriber{}

	err := n.insert(topic, 1, sub1)

	require.NoError(t, err)
	require.Equal(t, 1, len(n.nodes))
	require.Equal(t, 0, len(n.subs))

	n2, ok := n.nodes["sport"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.nodes))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.nodes["tennis"]

	require.True(t, ok)
	require.Equal(t, 1, len(n3.nodes))
	require.Equal(t, 0, len(n3.subs))

	n4, ok := n3.nodes["player1"]

	require.True(t, ok)
	require.Equal(t, 1, len(n4.nodes))
	require.Equal(t, 0, len(n4.subs))

	n5, ok := n4.nodes["#"]

	require.True(t, ok)
	require.Equal(t, 0, len(n5.nodes))
	require.Equal(t, 1, len(n5.subs))

	var e *subscriber

	e, ok = n5.subs[uintptr(unsafe.Pointer(sub1))]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.entry)
}

func TestSNodeInsert2(t *testing.T) {
	n := newSNode()
	topic := "#"

	sub1 := &types.Subscriber{}

	err := n.insert(topic, 1, sub1)

	require.NoError(t, err)
	require.Equal(t, 1, len(n.nodes))
	require.Equal(t, 0, len(n.subs))

	n2, ok := n.nodes["#"]

	require.True(t, ok)
	require.Equal(t, 0, len(n2.nodes))
	require.Equal(t, 1, len(n2.subs))

	var e *subscriber

	e, ok = n2.subs[uintptr(unsafe.Pointer(sub1))]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.entry)
}

func TestSNodeInsert3(t *testing.T) {
	n := newSNode()
	topic := "+/tennis/#"

	sub1 := &types.Subscriber{}

	err := n.insert(topic, 1, sub1)

	require.NoError(t, err)
	require.Equal(t, 1, len(n.nodes))
	require.Equal(t, 0, len(n.subs))

	n2, ok := n.nodes["+"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.nodes))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.nodes["tennis"]

	require.True(t, ok)
	require.Equal(t, 1, len(n3.nodes))
	require.Equal(t, 0, len(n3.subs))

	n4, ok := n3.nodes["#"]

	require.True(t, ok)
	require.Equal(t, 0, len(n4.nodes))
	require.Equal(t, 1, len(n4.subs))

	var e *subscriber

	e, ok = n4.subs[uintptr(unsafe.Pointer(sub1))]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.entry)
}

func TestSNodeInsert4(t *testing.T) {
	n := newSNode()
	topic := "/finance"

	sub1 := &types.Subscriber{}

	err := n.insert(topic, 1, sub1)

	require.NoError(t, err)
	require.Equal(t, 1, len(n.nodes))
	require.Equal(t, 0, len(n.subs))

	n2, ok := n.nodes["+"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.nodes))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.nodes["finance"]

	require.True(t, ok)
	require.Equal(t, 0, len(n3.nodes))
	require.Equal(t, 1, len(n3.subs))

	var e *subscriber

	e, ok = n3.subs[uintptr(unsafe.Pointer(sub1))]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.entry)
}

func TestSNodeInsertDup(t *testing.T) {
	n := newSNode()
	topic := "/finance"

	sub1 := &types.Subscriber{}

	err := n.insert(topic, 1, sub1)
	require.NoError(t, err)

	err = n.insert(topic, 1, sub1)
	require.NoError(t, err)

	require.NoError(t, err)
	require.Equal(t, 1, len(n.nodes))
	require.Equal(t, 0, len(n.subs))

	n2, ok := n.nodes["+"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.nodes))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.nodes["finance"]

	require.True(t, ok)
	require.Equal(t, 0, len(n3.nodes))
	require.Equal(t, 1, len(n3.subs))

	var e *subscriber

	e, ok = n3.subs[uintptr(unsafe.Pointer(sub1))]
	require.Equal(t, true, ok)
	require.Equal(t, sub1, e.entry)
}

func TestSNodeRemove1(t *testing.T) {
	n := newSNode()
	topic := "sport/tennis/player1/#"

	sub1 := &types.Subscriber{}

	err := n.insert(topic, 1, sub1)
	require.NoError(t, err)

	err = n.remove("sport/tennis/player1/#", sub1)
	require.NoError(t, err)

	require.Equal(t, 0, len(n.nodes))
	require.Equal(t, 0, len(n.subs))
}

func TestSNodeRemove2(t *testing.T) {
	n := newSNode()
	topic := "sport/tennis/player1/#"

	sub1 := &types.Subscriber{}

	err := n.insert(topic, 1, sub1)
	require.NoError(t, err)

	err = n.remove("sport/tennis/player1", sub1)
	require.EqualError(t, types.ErrNotFound, err.Error())
}

func TestSNodeRemove3(t *testing.T) {
	n := newSNode()
	topic := "sport/tennis/player1/#"

	sub1 := &types.Subscriber{}
	sub2 := &types.Subscriber{}

	err := n.insert(topic, 1, sub1)
	require.NoError(t, err)

	err = n.insert(topic, 1, sub2)
	require.NoError(t, err)

	err = n.remove("sport/tennis/player1/#", nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(n.nodes))
	require.Equal(t, 0, len(n.subs))
}

func TestSNodeMatch1(t *testing.T) {
	n := newSNode()
	topic := "sport/tennis/player1/#"

	sub1 := &types.Subscriber{}
	err := n.insert(topic, 1, sub1)
	require.NoError(t, err)

	var subs types.Subscribers

	err = n.match("sport/tennis/player1/anzel", 1, &subs)
	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
}

func TestSNodeMatch2(t *testing.T) {
	n := newSNode()
	topic := "sport/tennis/player1/#"

	sub1 := &types.Subscriber{}

	err := n.insert(topic, 1, sub1)
	require.NoError(t, err)

	var subs types.Subscribers

	err = n.match("sport/tennis/player1/anzel", 1, &subs)
	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
}

func TestSNodeMatch3(t *testing.T) {
	n := newSNode()
	topic := "sport/tennis/player1/#"

	sub1 := &types.Subscriber{}

	err := n.insert(topic, 2, sub1)
	require.NoError(t, err)

	var subs types.Subscribers

	err = n.match("sport/tennis/player1/anzel", 2, &subs)
	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
}

func TestSNodeMatch4(t *testing.T) {
	n := newSNode()

	sub1 := &types.Subscriber{}

	err := n.insert("sport/tennis/#", 2, sub1)
	require.NoError(t, err)

	var subs types.Subscribers

	err = n.match("sport/tennis/player1/anzel", 2, &subs)
	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
}

func TestSNodeMatch5(t *testing.T) {
	n := newSNode()

	sub1 := &types.Subscriber{}
	sub2 := &types.Subscriber{}

	err := n.insert("sport/tennis/+/anzel", 1, sub1)
	require.NoError(t, err)

	err = n.insert("sport/tennis/player1/anzel", 1, sub2)
	require.NoError(t, err)

	var subs types.Subscribers

	err = n.match("sport/tennis/player1/anzel", 1, &subs)
	require.NoError(t, err)
	require.Equal(t, 2, len(subs))
}

func TestSNodeMatch6(t *testing.T) {
	n := newSNode()

	sub1 := &types.Subscriber{}
	sub2 := &types.Subscriber{}

	err := n.insert("sport/tennis/#", 2, sub1)
	require.NoError(t, err)

	err = n.insert("sport/tennis", 1, sub2)
	require.NoError(t, err)

	var subs types.Subscribers

	err = n.match("sport/tennis/player1/anzel", 2, &subs)
	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
	require.Equal(t, sub1, subs[0])
}

func TestSNodeMatch7(t *testing.T) {
	n := newSNode()

	sub1 := &types.Subscriber{}

	err := n.insert("+/+", 2, sub1)
	require.NoError(t, err)

	var subs types.Subscribers

	err = n.match("/finance", 1, &subs)
	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
}

func TestSNodeMatch8(t *testing.T) {
	n := newSNode()

	sub1 := &types.Subscriber{}

	err := n.insert("/+", 2, sub1)
	require.NoError(t, err)

	var subs types.Subscribers

	err = n.match("/finance", 1, &subs)
	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
}

func TestSNodeMatch9(t *testing.T) {
	n := newSNode()

	sub1 := &types.Subscriber{}

	err := n.insert("+", 2, sub1)
	require.NoError(t, err)

	var subs types.Subscribers

	err = n.match("/finance", 1, &subs)

	require.NoError(t, err)
	require.Equal(t, 0, len(subs))
}

func TestRNodeInsertRemove(t *testing.T) {
	n := newRNode()

	// --- Insert msg1

	msg := newPublishMessageLarge("sport/tennis/player1/ricardo", 1)

	err := n.insert(msg.Topic(), msg)

	require.NoError(t, err)
	require.Equal(t, 1, len(n.nodes))
	require.Nil(t, n.msg)

	n2, ok := n.nodes["sport"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.nodes))
	require.Nil(t, n2.msg)

	n3, ok := n2.nodes["tennis"]

	require.True(t, ok)
	require.Equal(t, 1, len(n3.nodes))
	require.Nil(t, n3.msg)

	n4, ok := n3.nodes["player1"]

	require.True(t, ok)
	require.Equal(t, 1, len(n4.nodes))
	require.Nil(t, n4.msg)

	n5, ok := n4.nodes["ricardo"]

	require.True(t, ok)
	require.Equal(t, 0, len(n5.nodes))
	require.NotNil(t, n5.msg)
	require.Equal(t, msg.QoS(), n5.msg.QoS())
	require.Equal(t, msg.Topic(), n5.msg.Topic())
	require.Equal(t, msg.Payload(), n5.msg.Payload())

	// --- Insert msg2

	msg2 := newPublishMessageLarge("sport/tennis/player1/andre", 1)

	err = n.insert(msg2.Topic(), msg2)

	require.NoError(t, err)
	require.Equal(t, 2, len(n4.nodes))

	n6, ok := n4.nodes["andre"]

	require.True(t, ok)
	require.Equal(t, 0, len(n6.nodes))
	require.NotNil(t, n6.msg)
	require.Equal(t, msg2.QoS(), n6.msg.QoS())
	require.Equal(t, msg2.Topic(), n6.msg.Topic())

	// --- Remove

	err = n.remove("sport/tennis/player1/andre")
	require.NoError(t, err)
	require.Equal(t, 1, len(n4.nodes))
}

func TestRNodeMatch(t *testing.T) {
	n := newRNode()

	msg1 := newPublishMessageLarge("sport/tennis/ricardo/stats", 1)
	err := n.insert(msg1.Topic(), msg1)
	require.NoError(t, err)

	msg2 := newPublishMessageLarge("sport/tennis/andre/stats", 1)
	err = n.insert(msg2.Topic(), msg2)
	require.NoError(t, err)

	msg3 := newPublishMessageLarge("sport/tennis/andre/bio", 1)
	err = n.insert(msg3.Topic(), msg3)
	require.NoError(t, err)

	var msglist []*message.PublishMessage

	// ---

	err = n.match(msg1.Topic(), &msglist)

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.match(msg2.Topic(), &msglist)

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.match(msg3.Topic(), &msglist)

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.match("sport/tennis/andre/+", &msglist)

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.match("sport/tennis/andre/#", &msglist)

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.match("sport/tennis/+/stats", &msglist)

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.match("sport/tennis/#", &msglist)

	require.NoError(t, err)
	require.Equal(t, 3, len(msglist))
}

func newPublishMessageLarge(topic string, qos message.QosType) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetPayload(make([]byte, 1024))
	msg.SetTopic(topic) // nolint: errcheck
	msg.SetQoS(qos)     // nolint: errcheck

	return msg
}
