package ds

import (
	"sync"
	"testing"
	"time"
)

func TestSimpleQueueBasic(t *testing.T) {
	const size = 5
	q := NewSimpleQueue[int](size)
	testQueueCapacity(&q, size, t)
	testQueueSize(&q, 0, t)

	p1Err := q.Put(42)
	testPutErr(p1Err, t)
	testQueueCapacity(&q, size-1, t)
	testQueueSize(&q, 1, t)

	p2Err := q.Put(123)
	testPutErr(p2Err, t)
	testQueueCapacity(&q, size-2, t)
	testQueueSize(&q, 2, t)

	n1, err1 := q.Pop()
	testPop(n1, err1, 42, t)
	testQueueCapacity(&q, size-1, t)
	testQueueSize(&q, 1, t)

	n2, err2 := q.Pop()
	testPop(n2, err2, 123, t)
	testQueueCapacity(&q, size, t)
	testQueueSize(&q, 0, t)
}

func TestSimpeQueueEmpty(t *testing.T) {
	const size = 5
	q := NewSimpleQueue[string](size)
	testQueueCapacity(&q, size, t)
	testQueueSize(&q, 0, t)

	_, err1 := q.Pop()
	if err1 == nil {
		t.Error("Expected error on popping from empty queue, got nil")
	}
	if err1 != ErrQueueIsEmpty {
		t.Errorf("Expected queue is empty error, got: %s", err1.Error())
	}
	testQueueCapacity(&q, size, t)
	testQueueSize(&q, 0, t)
}

func TestSimpleQueueZeroSize(t *testing.T) {
	type X struct{ X int }
	const size = 0
	q := NewSimpleQueue[X](size)
	testQueueCapacity(&q, 0, t)
	testQueueSize(&q, 0, t)

	_, err1 := q.Pop()
	if err1 == nil {
		t.Error("Expected error on popping from empty queue, got nil")
	}
	if err1 != ErrQueueIsEmpty {
		t.Errorf("Expected queue is empty error, got: %s", err1.Error())
	}
	testQueueCapacity(&q, 0, t)
	testQueueSize(&q, 0, t)

	putErr := q.Put(X{X: 42})
	if putErr == nil {
		t.Error("Expected non-nil error from Put to queue with size of 0")
	}
	if putErr != ErrQueueIsFull {
		t.Errorf("Expected queue is full error, but got: %s", putErr.Error())
	}
	testQueueCapacity(&q, 0, t)
	testQueueSize(&q, 0, t)
}

func TestSimpeQueueFull(t *testing.T) {
	const size = 3
	q := NewSimpleQueue[int](size)
	testQueueCapacity(&q, size, t)
	testQueueSize(&q, 0, t)

	p1Err := q.Put(42)
	testPutErr(p1Err, t)
	testQueueCapacity(&q, size-1, t)
	testQueueSize(&q, 1, t)

	p2Err := q.Put(142)
	testPutErr(p2Err, t)
	testQueueCapacity(&q, size-2, t)
	testQueueSize(&q, 2, t)

	p3Err := q.Put(242)
	testPutErr(p3Err, t)
	testQueueCapacity(&q, size-3, t)
	testQueueSize(&q, 3, t)

	p4Err := q.Put(1213123)
	if p4Err == nil {
		t.Error("Expected non-nil error from Put on full queue")
	}
	if p4Err != ErrQueueIsFull {
		t.Errorf("Expected queue is full error, but got: %s", p4Err.Error())
	}
	testQueueCapacity(&q, size-3, t)
	testQueueSize(&q, 3, t)

	p5Err := q.Put(-123123)
	if p5Err == nil {
		t.Error("Expected non-nil error from Put on full queue")
	}
	if p5Err != ErrQueueIsFull {
		t.Errorf("Expected queue is full error, but got: %s", p5Err.Error())
	}
	testQueueCapacity(&q, size-3, t)
	testQueueSize(&q, 3, t)

	n1, popErr := q.Pop()
	testPop(n1, popErr, 42, t)
	testQueueCapacity(&q, size-2, t)
	testQueueSize(&q, 2, t)
}

func TestSimpleQueueConcurrent(t *testing.T) {
	const size = 3000000
	const chunkSize = 100000
	q := NewSimpleQueue[int](size)
	errChan := make(chan error, size)
	var wg sync.WaitGroup
	wg.Add(5)

	go listenOnErrors(errChan, t) // This will fail when errChan got an error sent over
	go putMany(&q, chunkSize, errChan, &wg)
	go putMany(&q, chunkSize, errChan, &wg)
	go putMany(&q, chunkSize, errChan, &wg)
	go putMany(&q, chunkSize, errChan, &wg)

	// just to be sure there are enough objects on the queue before start popping
	time.Sleep(10 * time.Millisecond)
	go popMany(&q, chunkSize, errChan, &wg)

	wg.Wait()
	expectedCap := size - (4*chunkSize - 1*chunkSize)
	testQueueCapacity(&q, expectedCap, t)
	testQueueSize(&q, 4*chunkSize-1*chunkSize, t)
}

func BenchmarkQueuePutAndPop(b *testing.B) {
	const size = 1000
	type dagrun struct {
		RunId  string
		DagId  string
		TaskId string
	}
	q := NewSimpleQueue[dagrun](size)
	dr := dagrun{"runid_123123", "sample_dag", "print_task"}

	for i := 0; i < b.N; i++ {
		q.Put(dr)
		q.Pop()
	}
}

func BenchmarkQueuePut1000Objects(b *testing.B) {
	const size = 10000
	const putBatchSize = 1000
	type dagrun struct {
		RunId  string
		DagId  string
		TaskId string
	}
	q := NewSimpleQueue[dagrun](size)
	dr := dagrun{"runid_123123", "sample_dag", "print_task"}

	for i := 0; i < b.N; i++ {
		for j := 0; j < putBatchSize; j++ {
			q.Put(dr)
		}
	}
}

func testQueueCapacity[T comparable](q *SimpleQueue[T], expectedCapacity int, t *testing.T) {
	cap := q.Capacity()
	if cap != expectedCapacity {
		t.Errorf("Expected queue capacity %d, got: %d", expectedCapacity, cap)
	}
}

func testQueueSize[T comparable](q *SimpleQueue[T], expectedSize int, t *testing.T) {
	s := q.Size()
	if s != expectedSize {
		t.Errorf("Expected queue size %d, got: %d", expectedSize, s)
	}
}

func testPutErr(err error, t *testing.T) {
	if err != nil {
		t.Errorf("Unexpected error while putting element on the queue: %s", err.Error())
	}
}

func testPop[T comparable](popObj T, popErr error, expectedObj T, t *testing.T) {
	if popErr != nil {
		t.Errorf("Error while popping object from non-empty queue: %s", popErr.Error())
	}
	if popObj != expectedObj {
		t.Errorf("Expected pop obj to be %v, got: %v", expectedObj, popObj)
	}
}

func putMany(q *SimpleQueue[int], n int, errChan chan error, wg *sync.WaitGroup) {
	for i := 0; i < n; i++ {
		pErr := q.Put(i)
		if pErr != nil {
			errChan <- pErr
		}
	}
	wg.Done()
}

func popMany(q *SimpleQueue[int], n int, errChan chan error, wg *sync.WaitGroup) {
	for i := 0; i < n; i++ {
		_, popErr := q.Pop()
		if popErr != nil {
			errChan <- popErr
		}
	}
	wg.Done()
}

func listenOnErrors(errChan chan error, t *testing.T) {
	for {
		err := <-errChan
		t.Errorf("Err: %s", err.Error())
	}
}
