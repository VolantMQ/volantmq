package types_test

import (
	"testing"

	"github.com/VolantMQ/volantmq/types"
)

type one int

func (o *one) Increment() {
	*o++
}

func run(once *types.Once, o *one, c chan bool) {
	once.Do(func() { o.Increment() })
	c <- true
}

func runWait(t *testing.T, once *types.OnceWait, o *one, c chan bool) {
	once.Do(func() { o.Increment() })
	if v := *o; v != 1 {
		t.Errorf("once failed inside run: %d is not 1", v)
	}
	c <- true
}

func TestOnce(t *testing.T) {
	o := new(one)
	once := new(types.Once)
	c := make(chan bool)
	const N = 10
	for i := 0; i < N; i++ {
		go run(once, o, c)
	}
	for i := 0; i < N; i++ {
		<-c
	}
	if *o != 1 {
		t.Errorf("once failed outside run: %d is not 1", *o)
	}
}

func TestOncePanic(t *testing.T) {
	var once types.Once
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Once.Do did not panic")
			}
		}()
		once.Do(func() {
			panic("failed")
		})
	}()

	once.Do(func() {
		t.Fatalf("Once.Do called twice")
	})
}

func TestOnceWait(t *testing.T) {
	o := new(one)
	once := new(types.OnceWait)
	c := make(chan bool)
	const N = 10
	for i := 0; i < N; i++ {
		go runWait(t, once, o, c)
	}
	for i := 0; i < N; i++ {
		<-c
	}
	if *o != 1 {
		t.Errorf("once failed outside run: %d is not 1", *o)
	}
}

func BenchmarkOnce(b *testing.B) {
	var once types.Once
	f := func() {}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			once.Do(f)
		}
	})
}
