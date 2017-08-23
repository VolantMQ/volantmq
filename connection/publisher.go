package connection

import (
	"container/list"
	"sync"
)

type publisher struct {
	// signal publisher goroutine to exit on channel close
	quit chan struct{}
	// make sure writer started before exiting from Start()
	started sync.WaitGroup
	// make sure writer has finished before any finalization
	stopped  sync.WaitGroup
	lock     sync.Mutex
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
