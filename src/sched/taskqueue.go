package sched

import (
	"errors"
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
	Capacity() int
	Size() int
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

// Put puts given task at the end of the task queue.
func (stq *SimpleQueue[T]) Put(task T) error {
	stq.Lock()
	defer stq.Unlock()
	if len(stq.buffer) >= stq.maxSize {
		return ErrQueueIsFull
	}
	stq.buffer = append(stq.buffer, task)
	return nil
}

func (stq *SimpleQueue[T]) Pop() (T, error) {
	stq.Lock()
	defer stq.Unlock()
	if len(stq.buffer) == 0 {
		var t T
		return t, ErrQueueIsEmpty
	}
	task := stq.buffer[0]
	stq.buffer = stq.buffer[1:]
	return task, nil
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
