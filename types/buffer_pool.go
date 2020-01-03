package types

import (
	"bufio"
	"io"
	"sync"
)

type WritePool interface {
	Get(io.Reader) *bufio.Reader
	Put(*bufio.Reader)
}

type writePool struct {
	sync.Mutex
	size            int
	growSize        int
	shrinkSize      int
	growThreshold   int
	shrinkThreshold int
	buffers         []*bufio.Reader
}

var _ WritePool = (*writePool)(nil)

func NewWritePool(size, preAlloc, grow, shrink int) (WritePool, error) {
	pool := &writePool{
		size:            size,
		growSize:        grow,
		shrinkSize:      shrink,
		growThreshold:   calcThreshold(preAlloc, grow),
		shrinkThreshold: calcThreshold(preAlloc, shrink),
	}

	for i := 0; i < preAlloc; i++ {
		pool.buffers = append(pool.buffers, bufio.NewReaderSize(pool, size))
	}

	return pool, nil
}

func (b *writePool) Get(rd io.Reader) *bufio.Reader {
	b.Lock()

	if len(b.buffers) == 0 {
		// double amount of buffers
		sz := cap(b.buffers)

		var buffers []*bufio.Reader

		for i := 0; i < sz; i++ {
			buffers = append(buffers, bufio.NewReaderSize(b, b.size))
		}

		b.buffers = append(buffers, b.buffers...)
	}

	buf := b.buffers[len(b.buffers)-1]

	buf.Reset(rd)

	b.buffers = b.buffers[:len(b.buffers)-1]

	b.Unlock()

	return buf
}

func (b *writePool) Put(buf *bufio.Reader) {
	b.Lock()

	buf.Reset(b)

	b.buffers = append(b.buffers, buf)

	b.Unlock()
}

func (b *writePool) Read(p []byte) (n int, err error) {
	return 0, nil
}

// nolint:unused
func (b *writePool) grow() {

}

// nolint:unused
func (b *writePool) shrink() {

}

func calcThreshold(size, grow int) int {
	return (grow * size) / 100
}
