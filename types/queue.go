package types

import (
	"sync"
)

// minQueueLen is smallest capacity that queue may have.
// Must be power of 2 for bitwise modulus: x % n == x & (n - 1).
const minQueueLen = 16

// Queue represents a single instance of the queue data structure.
type Queue struct {
	buf   []interface{}
	head  int
	tail  int
	count int
	lock  sync.RWMutex
}

// NewQueue constructs and returns a new Queue.
func NewQueue() *Queue {
	return &Queue{
		buf: make([]interface{}, minQueueLen),
	}
}

// Length returns the number of elements currently stored in the queue.
func (q *Queue) Length() int {
	defer q.lock.RUnlock()
	q.lock.RLock()

	return q.count
}

// resize the queue to fit exactly twice its current contents
// this can result in shrinking if the queue is less than half-full
func (q *Queue) resize() {
	// defer q.lock.Unlock()
	// q.lock.Lock()

	newBuf := make([]interface{}, q.count<<1)

	if q.tail > q.head {
		copy(newBuf, q.buf[q.head:q.tail])
	} else {
		n := copy(newBuf, q.buf[q.head:])
		copy(newBuf[n:], q.buf[:q.tail])
	}

	q.head = 0
	q.tail = q.count
	q.buf = newBuf
}

// Add puts an element on the end of the queue.
func (q *Queue) Add(elem interface{}) {
	defer q.lock.Unlock()
	q.lock.Lock()

	if q.count == len(q.buf) {
		q.resize()
	}

	q.buf[q.tail] = elem
	// bitwise modulus
	q.tail = (q.tail + 1) & (len(q.buf) - 1)
	q.count++
}

// Peek returns the element at the head of the queue. This call panics
// if the queue is empty.
func (q *Queue) Peek() interface{} {
	defer q.lock.RUnlock()
	q.lock.RLock()

	if q.count == 0 {
		return nil
	}

	return q.buf[q.head]
}

// Get returns the element at index i in the queue. If the index is
// invalid, the call will panic. This method accepts both positive and
// negative index values. Index 0 refers to the first element, and
// index -1 refers to the last.
func (q *Queue) Get(i int) interface{} {
	defer q.lock.Unlock()
	q.lock.Lock()

	// If indexing backwards, convert to positive index.
	if i < 0 {
		i += q.count
	}

	if i < 0 || i >= q.count {
		panic("queue: Get() called with index out of range")
	}

	// bitwise modulus
	return q.buf[(q.head+i)&(len(q.buf)-1)]
}

// Remove removes and returns the element from the front of the queue. If the
// queue is empty, the call will panic.
func (q *Queue) Remove() interface{} {
	defer q.lock.Unlock()
	q.lock.Lock()

	if q.count == 0 {
		return nil
	}

	ret := q.buf[q.head]
	q.buf[q.head] = nil
	// bitwise modulus
	q.head = (q.head + 1) & (len(q.buf) - 1)
	q.count--

	// Resize down if buffer 1/4 full.
	if len(q.buf) > minQueueLen && (q.count<<2) == len(q.buf) {
		q.resize()
	}

	return ret
}
