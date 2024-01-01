package scheduler

import (
	"time"

	"github.com/dskrzypiec/scheduler/ds"
)

// Config represents main configuration for the Scheduler.
type Config struct {
	// DAG runs queue capacity. DAG run queue is preallocated on Scheduler
	// startup.
	DagRunQueueLen int

	// DAG run tasks queue capacity. DAG run tasks queue is preallocated on
	// Scheduler startup.
	DagRunTaskQueueLen int

	// Startup timeout duration. When scheduler call Start, it synchronize with
	// the database and possibly other resources. This duration interval is
	// setup in Start context.
	StartupContextTimeout time.Duration

	// Configuration for taskScheduler.
	TaskSchedulerConfig TaskSchedulerConfig

	// Configuration for dagRunWatcher
	DagRunWatcherConfig DagRunWatcherConfig
}

// Default Scheduler configuration.
var DefaultConfig Config = Config{
	DagRunQueueLen:      100,
	DagRunTaskQueueLen:  1000,
	TaskSchedulerConfig: DefaultTaskSchedulerConfig,
}

// Configuration for taskScheduler which is responsible for scheduling tasks
// for particular DAG run.
type TaskSchedulerConfig struct {
	// How long taskScheduler should wait in case when DAG run queue is empty.
	// Expressed in number of milliseconds.
	HeartbeatMs int

	// How often taskScheduler should check if all dependencies are met before
	// scheduling new task. Expressed in milliseconds.
	CheckDependenciesStatusMs int
}

// Default taskScheduler configuration.
var DefaultTaskSchedulerConfig TaskSchedulerConfig = TaskSchedulerConfig{
	HeartbeatMs:               1,
	CheckDependenciesStatusMs: 1,
}

// Configuration for dagRunWatcher which is responsible for scheduling new DAG
// runs based on their schedule.
type DagRunWatcherConfig struct {
	WatchIntervalMs       int
	QueueIsFullIntervalMs int
}

// Default dagRunWatcher configuration.
var DefaultDagRunWatcherConfig DagRunWatcherConfig = DagRunWatcherConfig{
	WatchIntervalMs:       100,
	QueueIsFullIntervalMs: 100,
}

// Queues contains queues internally needed by the Scheduler. It's
// exposed publicly, because those queues are of type ds.Queue which is a
// generic interface. This way one can link external queues like AWS SQS or
// others to be used internally by the Scheduler.
type Queues struct {
	DagRuns     ds.Queue[DagRun]
	DagRunTasks ds.Queue[DagRunTask]

	// TODO: queues for retries and recoveries from errors which weren't
	// related to task execution.
}

// Returns default instance of Queues which uses ds.SimpleQueue - fixed size
// buffer queues. Size of buffers are based on Config.
func DefaultQueues(config Config) Queues {
	dagRuns := ds.NewSimpleQueue[DagRun](config.DagRunQueueLen)
	tasks := ds.NewSimpleQueue[DagRunTask](config.DagRunTaskQueueLen)
	return Queues{
		DagRuns:     &dagRuns,
		DagRunTasks: &tasks,
	}
}
