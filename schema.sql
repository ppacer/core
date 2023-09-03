-- Table dagtasks represents tasks in dags. It contains history of changes. Current state of all DAGs and its tasks can
-- be determined by using IsCurrent=1 condition.
CREATE TABLE IF NOT EXISTS dagtasks (
    DagId TEXT NOT NULL,            -- DAG ID
    TaskId TEXT NOT NULL,           -- Task ID
    IsCurrent INT NOT NULL,         -- Flag if pair (DagId, TaskId) represents the current version
    InsertTs TEXT NOT NULL,         -- Insert timestamp in %Y-%m-%D %H:%M:%S format
    Version TEXT NOT NULL,          -- Scheduler Version
    TaskTypeName TEXT NOT NULL,     -- Go type name which implements this task
    TaskBodyHash TEXT NOT NULL,     -- Task Execute() method body source code hash
    TaskBodySource TEXT NOT NULL,   -- Task Execute() method body source code as text

    PRIMARY KEY (DagId, TaskId, IsCurrent)
);

