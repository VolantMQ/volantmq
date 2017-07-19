// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buffer

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

var (
	bufCNT int64
)

var (
	// ErrInsufficientData buffer has insufficient data
	ErrInsufficientData = errors.New("buffer has insufficient data")

	// ErrInsufficientSpace buffer has insufficient space
	ErrInsufficientSpace = errors.New("buffer has insufficient space")

	// ErrNotReady buffer is not ready yet
	ErrNotReady = errors.New("buffer is not ready")
)

const (
	// DefaultBufferSize buffer size created by default
	DefaultBufferSize = 1024 * 256
	// DefaultReadBlockSize default read block size
	DefaultReadBlockSize = 8192
	// DefaultWriteBlockSize default write block size
	DefaultWriteBlockSize = 8192
)

type sequence struct {
	// The current position of the producer or consumer
	cursor int64

	// The previous known position of the consumer (if producer) or producer (if consumer)
	gate int64

	// These are fillers to pad the cache line, which is generally 64 bytes
	//p2 int64
	//p3 int64
	//p4 int64
	//p5 int64
	//p6 int64
	//p7 int64
}

func newSequence() *sequence {
	return &sequence{}
}

func (b *sequence) get() int64 {
	return atomic.LoadInt64(&b.cursor)
}

func (b *sequence) set(seq int64) {
	atomic.StoreInt64(&b.cursor, seq)
}

// Type of buffer
// align atomic values to prevent panics on 32 bits macnines
// see https://github.com/golang/go/issues/5278
type Type struct {
	id   int64
	size int64
	mask int64
	done int64

	buf         []byte
	tmp         []byte
	ExternalBuf []byte

	pSeq  *sequence
	cSeq  *sequence
	pCond *sync.Cond
	cCond *sync.Cond
}

// New buffer
func New(size int64) (*Type, error) {
	if size < 0 {
		return nil, bufio.ErrNegativeCount
	}

	if size == 0 {
		size = DefaultBufferSize
	}

	if !powerOfTwo64(size) {
		return nil, fmt.Errorf("Size must be power of two. Try %d", roundUpPowerOfTwo64(size))
	}

	if size < 2*DefaultReadBlockSize {
		return nil, fmt.Errorf("Size must at least be %d. Try %d", 2*DefaultReadBlockSize, 2*DefaultReadBlockSize)
	}

	return &Type{
		id:          atomic.AddInt64(&bufCNT, 1),
		ExternalBuf: make([]byte, size),
		buf:         make([]byte, size),
		size:        size,
		mask:        size - 1,
		pSeq:        newSequence(),
		cSeq:        newSequence(),
		pCond:       sync.NewCond(new(sync.Mutex)),
		cCond:       sync.NewCond(new(sync.Mutex)),
	}, nil
}

// ID of buffer
func (b *Type) ID() int64 {
	return b.id
}

// Close buffer
func (b *Type) Close() error {
	atomic.StoreInt64(&b.done, 1)

	b.pCond.L.Lock()
	b.pCond.Broadcast()
	b.pCond.L.Unlock()

	b.pCond.L.Lock()
	b.cCond.Broadcast()
	b.pCond.L.Unlock()

	return nil
}

// Len of data
func (b *Type) Len() int {
	cpos := b.cSeq.get()
	ppos := b.pSeq.get()
	return int(ppos - cpos)
}

// Size of buffer
func (b *Type) Size() int64 {
	return b.size
}

// ReadFrom from reader
func (b *Type) ReadFrom(r io.Reader) (int64, error) {
	total := int64(0)

	for {
		if b.isDone() {
			return total, io.EOF
		}

		start, cnt, err := b.waitForWriteSpace(DefaultReadBlockSize)
		if err != nil {
			return 0, err
		}

		pStart := start & b.mask
		pEnd := pStart + int64(cnt)
		if pEnd > b.size {
			pEnd = b.size
		}

		n, err := r.Read(b.buf[pStart:pEnd])
		if n > 0 {
			total += int64(n)
			if _, err = b.WriteCommit(n); err != nil {
				return total, err
			}
		}

		if err != nil {
			return total, err
		}
	}
}

// WriteTo to writer
func (b *Type) WriteTo(w io.Writer) (int64, error) {
	total := int64(0)

	for {
		if b.isDone() {
			return total, io.EOF
		}

		p, err := b.ReadPeek(DefaultWriteBlockSize)
		// There's some data, let's process it first
		if len(p) > 0 {
			var n int
			n, err = w.Write(p)
			total += int64(n)

			if err != nil {
				return total, err
			}

			_, err = b.ReadCommit(n)
			if err != nil {
				return total, err
			}
		}

		if err != nil {
			if err != ErrInsufficientData {
				return total, err
			}
		}
	}
}

// Read data
func (b *Type) Read(p []byte) (int, error) {
	if b.isDone() && b.Len() == 0 {
		return 0, io.EOF
	}

	pl := int64(len(p))

	for {
		cPos := b.cSeq.get()
		pPos := b.pSeq.get()
		cIndex := cPos & b.mask

		// If consumer position is at least len(p) less than producer position, that means
		// we have enough data to fill p. There are two scenarios that could happen:
		// 1. cIndex + len(p) < buffer size, in this case, we can just copy() data from
		//    buffer to p, and copy will just copy enough to fill p and stop.
		//    The number of bytes copied will be len(p).
		// 2. cIndex + len(p) > buffer size, this means the data will wrap around to the
		//    the beginning of the buffer. In thise case, we can also just copy data from
		//    buffer to p, and copy will just copy until the end of the buffer and stop.
		//    The number of bytes will NOT be len(p) but less than that.
		if cPos+pl < pPos {
			n := copy(p, b.buf[cIndex:])

			b.cSeq.set(cPos + int64(n))
			b.pCond.L.Lock()
			b.pCond.Broadcast()
			b.pCond.L.Unlock()

			return n, nil
		}

		// If we got here, that means there's not len(p) data available, but there might
		// still be data.

		// If cPos < pPos, that means there's at least pPos-cPos bytes to read. Let's just
		// send that back for now.
		if cPos < pPos {
			// n bytes available
			avail := pPos - cPos

			// bytes copied
			var n int

			// if cIndex+n < size, that means we can copy all n bytes into p.
			// No wrapping in this case.
			if cIndex+avail < b.size {
				n = copy(p, b.buf[cIndex:cIndex+avail])
			} else {
				// If cIndex+n >= size, that means we can copy to the end of buffer
				n = copy(p, b.buf[cIndex:])
			}

			b.cSeq.set(cPos + int64(n))
			b.pCond.L.Lock()
			b.pCond.Broadcast()
			b.pCond.L.Unlock()
			return n, nil
		}

		// If we got here, that means cPos >= pPos, which means there's no data available.
		// If so, let's wait...

		b.cCond.L.Lock()
		for pPos = b.pSeq.get(); cPos >= pPos; pPos = b.pSeq.get() {
			if b.isDone() {
				b.cCond.L.Unlock()
				return 0, io.EOF
			}

			//b.cWait++
			b.cCond.Wait()
		}
		b.cCond.L.Unlock()
	}
}

// Write message
func (b *Type) Write(p []byte) (int, error) {
	if b.isDone() {
		return 0, io.EOF
	}

	start, _, err := b.waitForWriteSpace(len(p))
	if err != nil {
		return 0, err
	}

	// If we are here that means we now have enough space to write the full p.
	// Let's copy from p into this.buf, starting at position ppos&this.mask.
	total := ringCopy(b.buf, p, start&b.mask)

	b.pSeq.set(start + int64(len(p)))
	b.cCond.L.Lock()
	b.cCond.Broadcast()
	b.cCond.L.Unlock()

	return total, nil
}

// ReadPeek Description below is copied completely from bufio.Peek()
//   http://golang.org/pkg/bufio/#Reader.Peek
// Peek returns the next n bytes without advancing the reader. The bytes stop being valid
// at the next read call. If Peek returns fewer than n bytes, it also returns an error
// explaining why the read is short. The error is bufio.ErrBufferFull if n is larger than
// b's buffer size.
// If there's not enough data to peek, error is ErrBufferInsufficientData.
// If n < 0, error is bufio.ErrNegativeCount
func (b *Type) ReadPeek(n int) ([]byte, error) {
	if int64(n) > b.size {
		return nil, bufio.ErrBufferFull
	}

	if n < 0 {
		return nil, bufio.ErrNegativeCount
	}

	cPos := b.cSeq.get()
	pPos := b.pSeq.get()

	// If there's no data, then let's wait until there is some data
	b.cCond.L.Lock()
	for ; cPos >= pPos; pPos = b.pSeq.get() {
		if b.isDone() {
			b.cCond.L.Unlock()
			return nil, io.EOF
		}

		//b.cWait++
		b.cCond.Wait()
	}
	b.cCond.L.Unlock()

	// m = the number of bytes available. If m is more than what's requested (n),
	// then we make m = n, basically peek max n bytes
	m := pPos - cPos
	err := error(nil)

	if m >= int64(n) {
		m = int64(n)
	} else {
		err = ErrInsufficientData
	}

	// There's data to peek. The size of the data could be <= n.
	if cPos+m <= pPos {
		cindex := cPos & b.mask

		// If cindex (index relative to buffer) + n is more than buffer size, that means
		// the data wrapped
		if cindex+m > b.size {
			// reset the tmp buffer
			b.tmp = b.tmp[0:0]

			l := len(b.buf[cindex:])
			b.tmp = append(b.tmp, b.buf[cindex:]...)
			b.tmp = append(b.tmp, b.buf[0:m-int64(l)]...)
			return b.tmp, err
		}

		return b.buf[cindex : cindex+m], err
	}

	return nil, ErrInsufficientData
}

// ReadWait waits for for n bytes to be ready. If there's not enough data, then it will
// wait until there's enough. This differs from ReadPeek or Readin that Peek will
// return whatever is available and won't wait for full count.
func (b *Type) ReadWait(n int) ([]byte, error) {
	if int64(n) > b.size {
		return nil, bufio.ErrBufferFull
	}

	if n < 0 {
		return nil, bufio.ErrNegativeCount
	}

	cPos := b.cSeq.get()
	pPos := b.pSeq.get()

	// This is the magic read-to position. The producer position must be equal or
	// greater than the next position we read to.
	next := cPos + int64(n)

	// If there's no data, then let's wait until there is some data
	b.cCond.L.Lock()
	for ; next > pPos; pPos = b.pSeq.get() {
		if b.isDone() {
			b.cCond.L.Unlock()
			return nil, io.EOF
		}

		b.cCond.Wait()
	}
	b.cCond.L.Unlock()

	//if b.isDone() {
	//	return nil, io.EOF
	//}

	// If we are here that means we have at least n bytes of data available.
	cIndex := cPos & b.mask

	// If cIndex (index relative to buffer) + n is more than buffer size, that means
	// the data wrapped
	if cIndex+int64(n) > b.size {
		// reset the tmp buffer
		b.tmp = b.tmp[0:0]

		l := len(b.buf[cIndex:])
		b.tmp = append(b.tmp, b.buf[cIndex:]...)
		b.tmp = append(b.tmp, b.buf[0:n-l]...)
		return b.tmp[:n], nil
	}

	return b.buf[cIndex : cIndex+int64(n)], nil
}

// ReadCommit Commit moves the cursor forward by n bytes. It behaves like Read() except it doesn't
// return any data. If there's enough data, then the cursor will be moved forward and
// n will be returned. If there's not enough data, then the cursor will move forward
// as much as possible, then return the number of positions (bytes) moved.
func (b *Type) ReadCommit(n int) (int, error) {
	if int64(n) > b.size {
		return 0, bufio.ErrBufferFull
	}

	if n < 0 {
		return 0, bufio.ErrNegativeCount
	}

	cPos := b.cSeq.get()
	pPos := b.pSeq.get()

	// If consumer position is at least n less than producer position, that means
	// we have enough data to fill p. There are two scenarios that could happen:
	// 1. cindex + n < buffer size, in this case, we can just copy() data from
	//    buffer to p, and copy will just copy enough to fill p and stop.
	//    The number of bytes copied will be len(p).
	// 2. cindex + n > buffer size, this means the data will wrap around to the
	//    the beginning of the buffer. In thise case, we can also just copy data from
	//    buffer to p, and copy will just copy until the end of the buffer and stop.
	//    The number of bytes will NOT be len(p) but less than that.
	if cPos+int64(n) <= pPos {
		b.cSeq.set(cPos + int64(n))
		b.pCond.L.Lock()
		b.pCond.Broadcast()
		b.pCond.L.Unlock()
		return n, nil
	}

	return 0, ErrInsufficientData
}

// WriteWait waits for n bytes to be available in the buffer and then returns
// 1. the slice pointing to the location in the buffer to be filled
// 2. a boolean indicating whether the bytes available wraps around the ring
// 3. any errors encountered. If there's error then other return values are invalid
func (b *Type) WriteWait(n int) ([]byte, bool, error) {
	start, cnt, err := b.waitForWriteSpace(n)
	if err != nil {
		return nil, false, err
	}

	pStart := start & b.mask
	if pStart+int64(cnt) > b.size {
		return b.buf[pStart:], true, nil
	}

	return b.buf[pStart : pStart+int64(cnt)], false, nil
}

// WriteCommit write with commit
func (b *Type) WriteCommit(n int) (int, error) {
	start, cnt, err := b.waitForWriteSpace(n)
	if err != nil {
		return 0, err
	}

	// If we are here then there's enough bytes to commit
	b.pSeq.set(start + int64(cnt))

	b.cCond.L.Lock()
	b.cCond.Broadcast()
	b.cCond.L.Unlock()

	return cnt, nil
}

// Send to
func (b *Type) Send(from [][]byte) (int, error) {
	defer func() {
		if int64(len(b.ExternalBuf)) > b.size {
			b.ExternalBuf = make([]byte, b.size)
		}
	}()

	var total int

	for _, s := range from {
		remaining := len(s)
		offset := 0
		for remaining > 0 {
			toWrite := remaining
			if toWrite > int(b.Size()) {
				toWrite = int(b.Size())
			}

			var wrote int
			var err error

			if wrote, err = b.Write(s[offset : offset+toWrite]); err != nil {
				return 0, err
			}

			remaining -= wrote
			offset += wrote
		}
		total += len(s)
	}

	return total, nil
}

func (b *Type) waitForWriteSpace(n int) (int64, int, error) {
	if b.isDone() {
		return 0, 0, io.EOF
	}

	// The current producer position, remember it's a forever inreasing int64,
	// NOT the position relative to the buffer
	pPos := b.pSeq.get()

	// The next producer position we will get to if we write len(p)
	next := pPos + int64(n)

	// For the producer, gate is the previous consumer sequence.
	gate := b.pSeq.gate

	wrap := next - b.size

	// If wrap point is greater than gate, that means the consumer hasn't read
	// some of the data in the buffer, and if we read in additional data and put
	// into the buffer, we would overwrite some of the unread data. It means we
	// cannot do anything until the customers have passed it. So we wait...
	//
	// Let's say size = 16, block = 4, pPos = 0, gate = 0
	//   then next = 4 (0+4), and wrap = -12 (4-16)
	//   _______________________________________________________________________
	//   | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 |
	//   -----------------------------------------------------------------------
	//    ^                ^
	//    pPos,            next
	//    gate
	//
	// So wrap (-12) > gate (0) = false, and gate (0) > pPos (0) = false also,
	// so we move on (no waiting)
	//
	// Now if we get to pPos = 14, gate = 12,
	// then next = 18 (4+14) and wrap = 2 (18-16)
	//
	// So wrap (2) > gate (12) = false, and gate (12) > pPos (14) = false aos,
	// so we move on again
	//
	// Now let's say we have pPos = 14, gate = 0 still (nothing read),
	// then next = 18 (4+14) and wrap = 2 (18-16)
	//
	// So wrap (2) > gate (0) = true, which means we have to wait because if we
	// put data into the slice to the wrap point, it would overwrite the 2 bytes
	// that are currently unread.
	//
	// Another scenario, let's say pPos = 100, gate = 80,
	// then next = 104 (100+4) and wrap = 88 (104-16)
	//
	// So wrap (88) > gate (80) = true, which means we have to wait because if we
	// put data into the slice to the wrap point, it would overwrite the 8 bytes
	// that are currently unread.
	//
	if wrap > gate || gate > pPos {
		var cPos int64
		b.pCond.L.Lock()
		for cPos = b.cSeq.get(); wrap > cPos; cPos = b.cSeq.get() {
			if b.isDone() {
				return 0, 0, io.EOF
			}

			//b.pWait++
			b.pCond.Wait()
		}

		b.pSeq.gate = cPos
		b.pCond.L.Unlock()
	}

	return pPos, n, nil
}

func (b *Type) isDone() bool {
	return atomic.LoadInt64(&b.done) == 1
}

func ringCopy(dst, src []byte, start int64) int {
	n := len(src)

	var i int
	var l int

	for n > 0 {
		l = copy(dst[start:], src[i:])
		i += l
		n -= l

		if n > 0 {
			start = 0
		}
	}

	return i
}

func powerOfTwo64(n int64) bool {
	return n != 0 && (n&(n-1)) == 0
}

func roundUpPowerOfTwo64(n int64) int64 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}
