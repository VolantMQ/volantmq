package omap

import "fmt"

// KVPair represents tuple
type KVPair struct {
	Key   interface{}
	Value interface{}
}

// String representations of the key pair
func (k *KVPair) String() string {
	return fmt.Sprintf("%v:%v", k.Key, k.Value)
}

// Compare current key to another
func (k *KVPair) Compare(kv2 *KVPair) bool {
	return k.Key == kv2.Key && k.Value == kv2.Value
}
