package omap

import "fmt"

// Map object
type Map interface {
	Set(key interface{}, value interface{})
	Get(key interface{}) (interface{}, bool)
	Delete(key interface{})
	Iterator() func() (*KVPair, bool)
	Len() int
}

type impl struct {
	store  map[interface{}]interface{}
	mapper map[interface{}]*node
	root   *node
}

// New map object
func New() Map {
	om := &impl{
		store:  make(map[interface{}]interface{}),
		mapper: make(map[interface{}]*node),
		root:   newRootNode(),
	}
	return om
}

func (om *impl) Set(key interface{}, value interface{}) {
	if _, ok := om.store[key]; !ok {
		root := om.root
		last := root.Prev
		last.Next = newNode(last, root, key)
		root.Prev = last.Next
		om.mapper[key] = last.Next
	}
	om.store[key] = value
}

func (om *impl) Get(key interface{}) (interface{}, bool) {
	val, ok := om.store[key]
	return val, ok
}

func (om *impl) Delete(key interface{}) {
	_, ok := om.store[key]
	if ok {
		delete(om.store, key)
	}
	root, rootFound := om.mapper[key]
	if rootFound {
		prev := root.Prev
		next := root.Next
		prev.Next = next
		next.Prev = prev
	}
}

func (om *impl) String() string {
	builder := make([]string, len(om.store))

	var index int
	iter := om.Iterator()
	for kv, ok := iter(); ok; kv, ok = iter() {
		val, _ := om.Get(kv.Key)
		builder[index] = fmt.Sprintf("%v:%v, ", kv.Key, val)
		index++
	}
	return fmt.Sprintf("Map%v", builder)
}

/*
Beware, Iterator leaks goroutines if we do not fully traverse the map.
For most cases, `IterFunc()` should work as an iterator.
*/
//func (om *impl) UnsafeIter() <-chan *KVPair {
//	keys := make(chan *KVPair)
//	go func() {
//		defer close(keys)
//		var curr *node
//		root := om.root
//		curr = root.Next
//		for curr != root {
//			v, _ := om.store[curr.Value]
//			keys <- &KVPair{curr.Value, v}
//			curr = curr.Next
//		}
//	}()
//	return keys
//}

func (om *impl) Iterator() func() (*KVPair, bool) {
	var curr *node
	root := om.root
	curr = root.Next
	return func() (*KVPair, bool) {
		for curr != root {
			tmp := curr
			curr = curr.Next
			v := om.store[tmp.Value]
			return &KVPair{tmp.Value, v}, true
		}
		return nil, false
	}
}

func (om *impl) Len() int {
	return len(om.store)
}
