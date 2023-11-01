package sched

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dskrzypiec/scheduler/src/db"
	"github.com/dskrzypiec/scheduler/src/timeutils"
)

var (
	ErrCacheKeyExists       = errors.New("given key already exists in the cache")
	ErrCacheKeyDoesNotExist = errors.New("given key does not exist in the cache")
)

type cacheableKeys interface {
	DagRun | DagRunTask
}

type cacheableValues interface {
	DagRunTaskState
}

// Cache is a generic interface for a cache with additional logic to fall back
// to database and get missing entry.
type cache[K cacheableKeys, V cacheableValues] interface {
	Add(key K, val V) error
	Get(key K) (V, bool)
	Remove(key K)
	Update(key K, newValue V) error
	Len() int
	PullFromDatabase(ctx context.Context, key K, dbClient *db.Client) error
}

// SimpleCache implements cache to reduce database load for most common data
// used in scheduler.
type simpleCache[K cacheableKeys, V cacheableValues] struct {
	sync.Mutex
	data map[K]V
}

// Creates new instance of simpleCache.
func newSimpleCache[K cacheableKeys, V cacheableValues]() simpleCache[K, V] {
	return simpleCache[K, V]{
		data: map[K]V{},
	}
}

// Add new entry into the cache. If given key already exists, then
// ErrCacheKeyExists is returned.
func (sc *simpleCache[K, V]) Add(key K, val V) error {
	sc.Lock()
	defer sc.Unlock()
	if _, exists := sc.data[key]; exists {
		return ErrCacheKeyExists
	}
	sc.data[key] = val
	return nil
}

// Get gets value for given key. If key is not present in the cache, then
// the second return variable will be false, like in case of map[K]V.
func (sc *simpleCache[K, V]) Get(key K) (V, bool) {
	sc.Lock()
	defer sc.Unlock()
	res, exists := sc.data[key]
	return res, exists
}

// Remove removes given key from the cache. If key does not exist it does
// nothing.
func (sc *simpleCache[K, V]) Remove(key K) {
	sc.Lock()
	defer sc.Unlock()
	delete(sc.data, key)
}

// Update updates existing entry in the cache for given key and new value.
// Return ErrCacheKeyDoesNotExist in case when given key is not in the cache.
func (sc *simpleCache[K, V]) Update(key K, newVal V) error {
	sc.Lock()
	defer sc.Unlock()
	if _, exists := sc.data[key]; exists {
		sc.data[key] = newVal
		return nil
	}
	return ErrCacheKeyDoesNotExist
}

// Len returns number of items in the cache.
func (sc *simpleCache[K, V]) Len() int {
	sc.Lock()
	defer sc.Unlock()
	return len(sc.data)
}

// PullFromDatabase pulls data to be put into the cache based on type of given
// key. TODO: more details.
func (sc *simpleCache[K, V]) PullFromDatabase(
	ctx context.Context,
	key K,
	dbClient *db.Client,
) error {
	switch obj := any(key).(type) {
	case DagRunTask:
		dagruntask, err := dbClient.ReadDagRunTask(
			ctx, string(obj.DagId), timeutils.ToString(obj.AtTime), obj.TaskId,
		)
		if err != nil {
			return err
		}
		status, sErr := stringToDagRunTaskStatus(dagruntask.Status)
		if sErr != nil {
			return sErr
		}
		v := DagRunTaskState{
			Status:         status,
			StatusUpdateTs: timeutils.FromStringMust(dagruntask.StatusUpdateTs),
		}

		if _, exists := sc.Get(key); !exists {
			return sc.Add(key, any(v).(V))
		}
		return sc.Update(key, any(v).(V))
	default:
		return fmt.Errorf("unsupported key type given in PullFromDatabase")
	}
}
