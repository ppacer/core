package ds

import "testing"

func TestLruCacheSimple(t *testing.T) {
	c := NewLruCache[int, string](2)

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

	// after adding key -123 the first key 42 should be removed already
	_, existsAfter3 := c.Get(42)
	if existsAfter3 {
		t.Errorf("Expected 42 to be removed from the cache, but it's still there")
	}
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

// TODO: add test for concurrent access

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
