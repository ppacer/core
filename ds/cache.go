package ds

import (
	"container/list"
	"sync"
)

// Cache is generic interface for a cache.
type Cache[K comparable, V any] interface {
	Get(key K) (V, bool)
	Put(key K, value V)
	Remove(key K)
	Len() int
}

// Least Recently Used cache implementation of Cache interface. It's safe for
// concurrent use.
type LruCache[K comparable, V any] struct {
	lock     sync.RWMutex
	items    map[K]*list.Element
	queue    *list.List
	capacity int
}

// NewLruCache returns an initialized LRU cache of given capacity.
func NewLruCache[K comparable, V any](capacity int) *LruCache[K, V] {
	return &LruCache[K, V]{
		capacity: capacity,
		items:    make(map[K]*list.Element),
		queue:    list.New(),
	}
}

type lruCacheItem[K comparable, V any] struct {
	key   K
	value V
}

// Get gets value from the cache for given key. If given key does not exist,
// then the second return value is false.
func (lc *LruCache[K, V]) Get(key K) (V, bool) {
	lc.lock.RLock()
	defer lc.lock.RUnlock()
	if element, found := lc.items[key]; found {
		lc.queue.MoveToFront(element)
		return element.Value.(*lruCacheItem[K, V]).value, true
	}
	var zero V
	return zero, false
}

// Put puts given value under given key into the cache. If key already exists,
// then it's value is updated.
func (lc *LruCache[K, V]) Put(key K, value V) {
	lc.lock.Lock()
	defer lc.lock.Unlock()
	if element, found := lc.items[key]; found {
		lc.queue.MoveToFront(element)
		element.Value.(*lruCacheItem[K, V]).value = value
		return
	}
	if lc.queue.Len() == lc.capacity {
		oldest := lc.queue.Back()
		if oldest != nil {
			lc.queue.Remove(oldest)
			delete(lc.items, oldest.Value.(*lruCacheItem[K, V]).key)
		}
	}
	item := &lruCacheItem[K, V]{key: key, value: value}
	lc.items[key] = lc.queue.PushFront(item)
}

// Len returns number of items stored in the cache.
func (lc *LruCache[K, V]) Len() int {
	lc.lock.RLock()
	defer lc.lock.RUnlock()
	return len(lc.items)
}

// Remove deletes given key from the cache. If given key is not present in the
// cache, then nothing happens.
func (lc *LruCache[K, V]) Remove(key K) {
	lc.lock.Lock()
	defer lc.lock.Unlock()
	if element, exists := lc.items[key]; exists {
		lc.queue.Remove(element)
		delete(lc.items, key)
	}
}
