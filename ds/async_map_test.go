package ds

import (
	"fmt"
	"sync"
	"testing"
)

func TestAsyncMapSimple(t *testing.T) {
	am := NewAsyncMap[string, int]()
	am.Add("test", 42)
	am.Add("test_2", 123)
	am.Add("test", -42)

	if am.Len() != 2 {
		t.Errorf("Expected 2 elements in the map, got: %d", am.Len())
	}
	val, exists := am.Get("test")
	if !exists {
		t.Error("Expected 'test' key to exists in the map, but it's not there")
	}
	if val != -42 {
		t.Errorf("Expected value -42 for 'test', got: %d", val)
	}
	am.Delete("test_2")

	if am.Len() != 1 {
		t.Errorf("Expected 1 element in the map, got: %d", am.Len())
	}
}

func TestAsyncMapSimpleAsync(t *testing.T) {
	am := NewAsyncMap[int, string]()
	var wg sync.WaitGroup
	const N = 10
	const size = 1000
	wg.Add(N)

	for i := 0; i < N; i++ {
		go addAsync(am, 0, size, &wg)
	}
	wg.Wait()

	if am.Len() != size {
		t.Errorf("Expected map size %d, got: %d", size, am.Len())
	}
}

func TestAsyncMapFromMapSimple(t *testing.T) {
	m := map[int]string{
		42:   "42",
		-123: "-123",
	}
	am := NewAsyncMapFromMap(m)

	if am.Len() != 2 {
		t.Errorf("Expected 2 elements in the map, got: %d", am.Len())
	}
	val, exists := am.Get(42)
	if !exists {
		t.Error("Expected 42 key to exists in the map, but it's not there")
	}
	if val != "42" {
		t.Errorf("Expected value '42' for 42, got: %s", val)
	}
	am.Delete(42)

	if am.Len() != 1 {
		t.Errorf("Expected 1 element in the map, got: %d", am.Len())
	}
}

func addAsync(am *AsyncMap[int, string], id int, n int, wg *sync.WaitGroup) {
	for i := 0; i < n; i++ {
		am.Add(i, fmt.Sprintf("value_from_%d", id))
	}
	wg.Done()
}
