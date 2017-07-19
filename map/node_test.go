package omap

import (
	"testing"
)

func testString() []string {
	var data = make([]string, 5)
	data[0] = "test0"
	data[1] = "test1"
	data[2] = "test2"
	data[3] = "test3"
	data[4] = "test4"
	return data
}

func TestAddData(t *testing.T) {
	expected := testString()

	ls := newRootNode()
	if ls == nil {
		t.Error("Failed to create LinkList")
	}

	for _, v := range expected {
		ls.Add(v)
	}

	index := 0
	iter := ls.IterFunc()
	for v, ok := iter(); ok; v, ok = iter() {
		if v != expected[index] {
			t.Error("Failed insert of args:", v, expected[index])
		}
		index++
	}
}
