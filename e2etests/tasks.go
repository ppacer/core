package e2etests

import "sync"

// Empty task with no action.
type emptyTask struct {
	taskId string
}

func (et emptyTask) Id() string { return et.taskId }
func (et emptyTask) Execute()   {}

// Task which writes messages to internal string slice.
type writeToSliceTask struct {
	taskId string
	sync.Mutex
	data []string
}

func (wst *writeToSliceTask) Id() string { return wst.taskId }

func (wst *writeToSliceTask) Execute() {
	wst.Lock()
	defer wst.Unlock()
	wst.data = append(wst.data, wst.taskId)
}
