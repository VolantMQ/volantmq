package connection

import (
	"sync"

	"container/list"
)

type publisher struct {
	// signal publisher goroutine to exit on channel close
	quit chan struct{}
	// make sure writer started before exiting from Start()
	started sync.WaitGroup
	// make sure writer has finished before any finalization
	stopped  sync.WaitGroup
	messages *list.List
	cond     *sync.Cond
}

func (p *publisher) isDone() bool {
	select {
	case <-p.quit:
		return true
	default:
	}

	return false
}

func newPublisher(quit chan struct{}) *publisher {
	return &publisher{
		messages: list.New(),
		cond:     sync.NewCond(new(sync.Mutex)),
		quit:     quit,
	}
}

func (p *publisher) waitForMessage() interface{} {
	if p.isDone() {
		return nil
	}

	defer p.cond.L.Unlock()
	p.cond.L.Lock()

	for p.messages.Len() == 0 {
		p.cond.Wait()
		if p.isDone() {
			return nil
		}
	}

	return p.messages.Remove(p.messages.Front())
}

func (p *publisher) pushFront(value interface{}) {
	p.cond.L.Lock()
	p.messages.PushFront(value)
	p.cond.L.Unlock()
	p.cond.Signal()
}

func (p *publisher) pushBack(value interface{}) {
	p.cond.L.Lock()
	p.messages.PushBack(value)
	p.cond.L.Unlock()
	p.cond.Signal()
}
