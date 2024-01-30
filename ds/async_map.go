// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package ds

import "sync"

// Analog of standard map but safe to use among many goroutines.
type AsyncMap[K comparable, V any] struct {
	sync.Mutex
	data map[K]V
}

// Instantiate new empty AsyncMap of given types.
func NewAsyncMap[K comparable, V any]() *AsyncMap[K, V] {
	return &AsyncMap[K, V]{
		data: map[K]V{},
	}
}

// Instantiate new AsyncMap based on standard map.
func NewAsyncMapFromMap[K comparable, V any](m map[K]V) *AsyncMap[K, V] {
	return &AsyncMap[K, V]{
		data: m,
	}
}

// Add value for a given key to the map. If given key already exists in the map
// it will be overwritten (consistent with standard map[K]V).
func (am *AsyncMap[K, V]) Add(key K, value V) {
	am.Lock()
	am.data[key] = value
	am.Unlock()
}

// Get gets value from the map for given key. If given key does not exists, the
// second return value will be false.
func (am *AsyncMap[K, V]) Get(key K) (V, bool) {
	am.Lock()
	defer am.Unlock()
	value, exists := am.data[key]
	return value, exists
}

// GetOrDefault gets value from the map for given key. If there is no entry in
// the map for given key, then default value is returned.
func (am *AsyncMap[K, V]) GetOrDefault(key K, defaultVal V) V {
	am.Lock()
	defer am.Unlock()
	value, exists := am.data[key]
	if !exists {
		return defaultVal
	}
	return value
}

// Delete deletes key and corresponding value from the map.
func (am *AsyncMap[K, V]) Delete(key K) {
	am.Lock()
	delete(am.data, key)
	am.Unlock()
}

// Len returns size of the map.
func (am *AsyncMap[K, V]) Len() int {
	am.Lock()
	defer am.Unlock()
	return len(am.data)
}
