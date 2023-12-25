package scheduler

// Config represents main configuration for the Scheduler.
type Config struct {
	// DAG runs queue capacity. DAG run queue is preallocated on Scheduler
	// startup.
	DagRunQueueLen int

	// DAG run tasks queue capacity. DAG run tasks queue is preallocated on
	// Scheduler startup.
	DagRunTaskQueueLen int

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
