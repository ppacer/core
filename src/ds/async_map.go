package ds

import "sync"

// Analog of standard map but safe to use among many goroutines.
type AsyncMap[K, V comparable] struct {
	sync.Mutex
	data map[K]V
}

func NewAsyncMap[K, V comparable]() *AsyncMap[K, V] {
	return &AsyncMap[K, V]{
		data: map[K]V{},
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
