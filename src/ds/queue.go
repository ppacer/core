// Package ds contains implementation for data structures.
package ds

import (
	"context"
	"errors"
	"log/slog"
	"sync"
)

var ErrQueueIsFull = errors.New("queue is full")
var ErrQueueIsEmpty = errors.New("queue is empty")

// Queue defines interface for the FIFO queue. Put puts object onto the queue,
// pop returns the first object from the queue. Capacity returns how many more
// objects can be put onto the queue before it will be full. Size method should
// return how much object there are currently on the queue. If object cannot be
// put, because the queue is full it Put method should return ErrQueueIsFull.
// In case when Pop method is called on empty queue, it should also return
// non-nil error ErrQueueIsEmpty.
type Queue[T comparable] interface {
	Put(t T) error
	Pop() (T, error)
	Contains(T) bool
	Capacity() int
	Size() int
}

// PutContext tries to put item onto the queue. In case of failures it tries
// again and again until either successfully put item onto the queue or context
// is done.
func PutContext[T comparable](ctx context.Context, q Queue[T], item T) {
	for {
		select {
		case <-ctx.Done():
			slog.Warn("Context is done before item could be put onto the queue",
				"item", item)
			return
		default:
		}
		putErr := q.Put(item)
		if putErr == nil {
			return
		}
	}
}

// Simple buffer-based FIFO queue. It's safe for concurrent use.
type SimpleQueue[T comparable] struct {
	maxSize int
	sync.Mutex
	buffer []T
}

func NewSimpleQueue[T comparable](queueMaxSize int) SimpleQueue[T] {
	return SimpleQueue[T]{
		maxSize: queueMaxSize,
		buffer:  make([]T, 0, queueMaxSize),
	}
}

// Put puts given object at the end of the queue. Returns ErrQueueIsFull is the
// queue is full and object cannot be put there.
func (stq *SimpleQueue[T]) Put(obj T) error {
	stq.Lock()
	defer stq.Unlock()
	if len(stq.buffer) >= stq.maxSize {
		return ErrQueueIsFull
	}
	stq.buffer = append(stq.buffer, obj)
	return nil
}

// Pop returns the first object from the queue and removes it from the queue.
// If the queue is empty, then non-nill error ErrQueueIsEmpty is returned.
func (stq *SimpleQueue[T]) Pop() (T, error) {
	stq.Lock()
	defer stq.Unlock()
	if len(stq.buffer) == 0 {
		var t T
		return t, ErrQueueIsEmpty
	}
	obj := stq.buffer[0]
	stq.buffer = stq.buffer[1:]
	return obj, nil
}

// Contains verifies whenever queue contains given element.
func (stq *SimpleQueue[T]) Contains(elem T) bool {
	stq.Lock()
	defer stq.Unlock()
	for _, item := range stq.buffer {
		if item == elem {
			return true
		}
	}
	return false
}

func (stq *SimpleQueue[T]) Capacity() int {
	stq.Lock()
	size := len(stq.buffer)
	stq.Unlock()
	return stq.maxSize - size
}

func (stq *SimpleQueue[T]) Size() int {
	stq.Lock()
	size := len(stq.buffer)
	stq.Unlock()
	return size
}
