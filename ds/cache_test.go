// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package ds

import (
	"sync"
	"testing"
)

func TestLruCacheSimple(t *testing.T) {
	c := NewLruCache[int, string](2)
	testCacheLen(c, 0, t)

	if _, exists := c.Get(42); exists {
		t.Errorf("Cache should be empty, but got key 42")
	}
	v1 := "test1"
	c.Put(42, v1)
	vc1, existsAfter := c.Get(42)
	if !existsAfter {
		t.Errorf("Expected value 42, to exist in the cache, but it's not")
	}
	if vc1 != v1 {
		t.Errorf("Expected %s from cache for key %d, but got: %s", v1, 42, vc1)
	}
	testCacheLen(c, 1, t)

	// Update
	v12 := "test2"
	c.Put(42, v12)
	vc12, exists := c.Get(42)
	if !exists {
		t.Errorf("Expected value 42, to exist in the cache, but it's not")
	}
	if vc12 != v12 {
		t.Errorf("Expected %s from cache for key %d, but got: %s", v12, 42,
			vc12)
	}
	testCacheLen(c, 1, t)

	// Another key
	v2 := ""
	c.Put(111, v2)
	vc2, exists := c.Get(111)
	if !exists {
		t.Errorf("Expected value 111, to exist in the cache, but it's not")
	}
	if vc2 != v2 {
		t.Errorf("Expected %s from cache for key %d, but got: %s", v2, 111,
			vc2)
	}
	testCacheLen(c, 2, t)

	// 3rd key - one over initial capacity
	v3 := "x"
	c.Put(-123, v3)
	vc3, exists := c.Get(-123)
	if !exists {
		t.Errorf("Expected value -123, to exist in the cache, but it's not")
	}
	if vc3 != v3 {
		t.Errorf("Expected %s from cache for key %d, but got: %s", v3, -123,
			vc3)
	}
	testCacheLen(c, 2, t)

	// after adding key -123 the first key 42 should be removed already
	_, existsAfter3 := c.Get(42)
	if existsAfter3 {
		t.Errorf("Expected 42 to be removed from the cache, but it's still there")
	}
	testCacheLen(c, 2, t)
}

func TestLruCacheRemovalOrder(t *testing.T) {
	c := NewLruCache[int, int](3)

	c.Put(1, 1)
	c.Put(2, 2)
	c.Put(3, 3)
	// 3 2 1

	c.Get(1)
	c.Get(3)
	c.Get(1)
	// 1 3 2

	c.Put(4, 4)
	// 4 1 3
	if _, exists := c.Get(2); exists {
		t.Errorf("Expected 2 to be removed from cache after 4 was inserted")
	}

	c.Get(3)
	c.Get(1)
	// 1 3 4

	c.Put(5, 5)
	// 5 1 3
	if _, exists := c.Get(4); exists {
		t.Errorf("Expected 4 to be removed from cache after 5 was inserted")
	}
}

func TestLruCacheRemoveItems(t *testing.T) {
	c := NewLruCache[string, string](3)
	c.Put("a", "x")
	c.Put("b", "y")
	c.Put("c", "z")
	testCacheLen(c, 3, t)
	c.Remove("this-key-does-not-exist")
	testCacheLen(c, 3, t)

	c.Remove("b")
	testCacheLen(c, 2, t)
	_, bExist := c.Get("b")
	if bExist {
		t.Error("Element with key 'b' should be deleted, but it's still in the cache")
	}

	c.Remove("a")
	testCacheLen(c, 1, t)
	_, aExist := c.Get("a")
	if aExist {
		t.Error("Element with key 'a' should be deleted, but it's still in the cache")
	}

	c.Remove("c")
	testCacheLen(c, 0, t)
	_, cExist := c.Get("c")
	if cExist {
		t.Error("Element with key 'c' should be deleted, but it's still in the cache")
	}

	c.Remove("this-key-does-not-exist")
	testCacheLen(c, 0, t)
}

func TestLruCacheConcurrency(t *testing.T) {
	const size = 1000
	c := NewLruCache[int, int](size)
	var wg sync.WaitGroup
	wg.Add(5)

	go putManyIntoCache(c, 0, 100, 0, &wg)
	go putManyIntoCache(c, 100, 200, 1, &wg)
	go putManyIntoCache(c, 200, 300, 2, &wg)
	go putManyIntoCache(c, 300, 400, 3, &wg)
	go putManyIntoCache(c, 400, 500, 4, &wg)

	wg.Wait()
	for i := 0; i < 500; i++ {
		v, exist := c.Get(i)
		if !exist {
			t.Errorf("Expected key %d to be in the cache, but it's not", i)
		}
		if v != i/100 {
			t.Errorf("Expected cache(%d)=%d, but got: %d", i, i/100, v)
		}
	}
}

func putManyIntoCache(c Cache[int, int], start, end, value int, wg *sync.WaitGroup) {
	for i := start; i < end; i++ {
		c.Put(i, value)
	}
	wg.Done()
}

func testCacheLen[K comparable, V any](c Cache[K, V], expCount int, t *testing.T) {
	cnt := c.Len()
	if cnt != expCount {
		t.Errorf("Expected %d items in the cache, but got: %d", expCount, cnt)
	}
}

func BenchmarkLruCachePut(b *testing.B) {
	const size = 100
	type dagruntask struct {
		RunId  string
		DagId  string
		TaskId string
	}

	cache := NewLruCache[dagruntask, string](size)
	drt := dagruntask{"runid_123", "sample_dag", "print_task"}

	for i := 0; i < b.N; i++ {
		cache.Put(drt, "running")
	}
}

func BenchmarkLruCacheGet(b *testing.B) {
	const size = 100
	type dagruntask struct {
		RunId  string
		DagId  string
		TaskId string
	}

	cache := NewLruCache[dagruntask, string](size)
	drt := dagruntask{"runid_123", "sample_dag", "print_task"}
	cache.Put(drt, "success")

	for i := 0; i < b.N; i++ {
		cache.Get(drt)
	}
}
