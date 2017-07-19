package omap

import (
	"testing"
)

func testStringInt() []*KVPair {
	var data = make([]*KVPair, 5)
	data[0] = &KVPair{"test0", 0}
	data[1] = &KVPair{"test1", 1}
	data[2] = &KVPair{"test2", 2}
	data[3] = &KVPair{"test3", 3}
	data[4] = &KVPair{"test4", 4}
	return data
}

func TestSetData(t *testing.T) {
	expected := testStringInt()
	om := New()
	if om == nil {
		t.Error("Failed to create OrderedMap")
	}

	m := om.(*impl)
	for _, kvp := range expected {
		om.Set(kvp.Key, kvp.Value)
	}

	if len(m.store) != len(expected) {
		t.Error("Failed insert of args:", m.store, expected)
	}
}

//func TestGetData(t *testing.T) {
//	data := testStringInt()
//	om := NewOrderedMapWithArgs(data)
//
//	for _, kvp := range data {
//		val, ok := om.Get(kvp.Key)
//		if ok && kvp.Value != val {
//			t.Error(kvp.Value, val)
//		}
//	}
//	_, ok := om.Get("invlalid-key")
//	if ok {
//		t.Error("Invalid key was found in OrderedMap")
//	}
//}
//
//func TestDeleteData(t *testing.T) {
//	data := testStringInt()
//	om := NewOrderedMapWithArgs(data)
//
//	testKey := data[2].Key
//
//	// First check to see if exists
//	_, ok := om.Get(testKey)
//	if !ok {
//		t.Error("Key/Value not found in OrderedMap")
//	}
//
//	// Delete key
//	om.Delete(testKey)
//
//	// Test to see if removed
//	_, ok2 := om.Get(testKey)
//	if ok2 {
//		t.Error("Key/Value was not deleted")
//	}
//}
//
//func TestIterator(t *testing.T) {
//	sample := testStringInt()
//	om := NewOrderedMapWithArgs(sample)
//	iter := om.UnsafeIter()
//	if iter == nil {
//		t.Error("Failed to create OrderedMap")
//	}
//
//	var index int = 0
//	for k := range iter {
//		expected := sample[index]
//		if !k.Compare(expected) {
//			t.Error(expected, k)
//		}
//		index++
//	}
//}
//
//func TestIteratorFunc(t *testing.T) {
//	sample := testStringInt()
//	om := NewOrderedMapWithArgs(sample)
//
//	iter := om.IterFunc()
//	if iter == nil {
//		t.Error("Failed to create OrderedMap")
//	}
//
//	var index int = 0
//	for k, ok := iter(); ok; k, ok = iter() {
//		expected := sample[index]
//		if !k.Compare(expected) {
//			t.Error(expected, k)
//		}
//		index++
//	}
//}
//
//func TestLenNonEmpty(t *testing.T) {
//	data := testStringInt()
//	om := NewOrderedMapWithArgs(data)
//
//	if om.Len() != len(data) {
//		t.Fatal("Unexpected length")
//	}
//}
//
//func TestLenEmpty(t *testing.T) {
//	om := NewOrderedMap()
//
//	if om.Len() != 0 {
//		t.Fatal("Unexpected length")
//	}
//}
