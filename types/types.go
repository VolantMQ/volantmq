package types

import (
	"github.com/troian/surgemq/message"
	"sync"
)

//type OnCompleteFunc func(msg, ack message.Provider, err error) error

// OnPublishFunc on publish
type OnPublishFunc func(msg *message.PublishMessage) error

// OnSessionClose session signal on it's close
//type OnSessionClose func(id uint64)

// Subscriber used inside each session as an object to provide to topic manager upon subscribe
type Subscriber struct {
	WgWriters sync.WaitGroup
	Publish   OnPublishFunc
}

// Subscribers used by topic manager to return list of subscribers matching topic
type Subscribers []*Subscriber
