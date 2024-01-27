package scheduler

import (
	"time"

	"github.com/ppacer/core/ds"
)

// Config represents main configuration for the Scheduler.
type Config struct {
	// DAG runs queue capacity. DAG run queue is preallocated on Scheduler
	// startup.
	DagRunQueueLen int

	// DAG run tasks queue capacity. DAG run tasks queue is preallocated on
	// Scheduler startup.
	DagRunTaskQueueLen int

	// DAG run tasks cache capacity.
	DagRunTaskCacheLen int

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
	DagRunQueueLen:        100,
	DagRunTaskQueueLen:    1000,
	DagRunTaskCacheLen:    1000,
	StartupContextTimeout: 30 * time.Second,
	TaskSchedulerConfig:   DefaultTaskSchedulerConfig,
	DagRunWatcherConfig:   DefaultDagRunWatcherConfig,
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

// Configuration for DagRunWatcher which is responsible for scheduling new DAG
// runs based on their schedule.
type DagRunWatcherConfig struct {
	// DagRunWatcher waits WatchInterval before another try of scheduling DAG
	// runs.
	WatchInterval time.Duration

	// DagRunWatcher would wait for QueueIsFullInterval in case when DAG run
	// queue is full, before it would try again.
	QueueIsFullInterval time.Duration

	// Duration for database context timeout for queries done by DagRunWatcher.
	DatabaseContextTimeout time.Duration
}

// Default DagRunWatcher configuration.
var DefaultDagRunWatcherConfig DagRunWatcherConfig = DagRunWatcherConfig{
	WatchInterval:          100 * time.Millisecond,
	QueueIsFullInterval:    100 * time.Millisecond,
	DatabaseContextTimeout: 10 * time.Second,
}

// Queues contains queues internally needed by the Scheduler. It's
// exposed publicly, because those queues are of type ds.Queue which is a
// generic interface. This way one can link external queues like AWS SQS or
// others to be used internally by the Scheduler.
type Queues struct {
	DagRuns     ds.Queue[DagRun]
	DagRunTasks ds.Queue[DagRunTask]
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

type ClientConfig struct {
	// HTTP client timeout value in case when http.Client needs to be
	// initialized within NewClient.
	HttpClientTimeout time.Duration
}

// Default Client configuration.
var DefaultClientConfig ClientConfig = ClientConfig{
	HttpClientTimeout: 15 * time.Second,
}
