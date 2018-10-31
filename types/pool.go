package types

import (
	"fmt"
	"time"
)

// ErrScheduleTimeout returned by Pool to indicate that there no free
// goroutines during some period of time.
var ErrScheduleTimeout = fmt.Errorf("pool: schedule error: timed out")

// ErrClosed notify object was stopped
var ErrClosed = fmt.Errorf("pool: closed")

// Pool implements logic of goroutine reuse.
type Pool interface {
	Schedule(task func()) error
	ScheduleTimeout(timeout time.Duration, task func()) error
	Close() error
}

type pool struct {
	quit chan struct{}
	sem  chan struct{}
	work chan func()
}

// NewPool creates new goroutine pool with given size. It also creates a work
// queue of given size. Finally, it spawns given amount of goroutines
// immediately.
func NewPool(size, queue, spawn int) Pool {
	if spawn <= 0 && queue > 0 {
		panic("dead queue configuration detected")
	}
	if spawn > size {
		panic("spawn > workers")
	}
	p := &pool{
		quit: make(chan struct{}),
		sem:  make(chan struct{}, size),
		work: make(chan func(), queue),
	}
	for i := 0; i < spawn; i++ {
		p.sem <- struct{}{}
		go p.worker(func() {})
	}

	return p
}

// Schedule schedules task to be executed over pool's workers.
func (p *pool) Schedule(task func()) error {
	select {
	case <-p.quit:
		return ErrClosed
	default:
	}
	return p.schedule(task, nil)
}

// ScheduleTimeout schedules task to be executed over pool's workers.
// It returns ErrScheduleTimeout when no free workers met during given timeout.
func (p *pool) ScheduleTimeout(timeout time.Duration, task func()) error {
	select {
	case <-p.quit:
		return ErrClosed
	default:
	}
	return p.schedule(task, time.After(timeout))
}

// Close shutdown pool
func (p *pool) Close() error {
	select {
	case <-p.quit:
	default:
		close(p.quit)
		close(p.sem)
		close(p.work)
	}
	return nil
}

func (p *pool) schedule(task func(), timeout <-chan time.Time) error {
	select {
	case <-timeout:
		return ErrScheduleTimeout
	case p.work <- task:
		return nil
	case p.sem <- struct{}{}:
		go p.worker(task)
		return nil
	}
}

func (p *pool) worker(task func()) {
	defer func() {
		<-p.sem
	}()

	task()

	for t := range p.work {
		t()
	}
}
