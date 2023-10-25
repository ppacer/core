-- Table dags stores DAGs and its metadata. Information about DAG tasks are stored in dagtasks table.
CREATE TABLE IF NOT EXISTS dags (
    DagId TEXT NOT NULL,            -- DAG ID
    StartTs TEXT NULL,              -- DAG start timestamp
    Schedule TEXT NULL,             -- DAG schedule
    CreateTs TEXT NOT NULL,         -- Timestamp when DAG was initially inserted
    LatestUpdateTs TEXT NULL,       -- Timestamp of the DAG latest update
    CreateVersion TEXT NOT NULL,    -- Verion when DAG was innitially inserted
    LatestUpdateVersion TEXT NULL,  -- Version of DAG latest update
    HashDagMeta TEXT NOT NULL,      -- SHA256 hash of DAG attributes + StartTs + Schedule
    HashTasks TEXT NOT NULL,        -- SHA256 hash of DAG tasks
    Attributes TEXT NOT NULL,       -- DAG attributes like tags
    -- TODO: probably many more, but sometime later

    PRIMARY KEY (DagId)
);

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

    PRIMARY KEY (DagId, TaskId, IsCurrent, InsertTs)
);

-- Table dagruns stores DAG runs information. Runs might be both scheduled for manually triggered.
CREATE TABLE IF NOT EXISTS dagruns (
    RunId INTEGER PRIMARY KEY,      -- Run ID - auto increments
    DagId TEXT NOT NULL,            -- DAG ID
    ExecTs TEXT NOT NULL,           -- Execution timestamp
    InsertTs TEXT NOT NULL,         -- Row insertion timestamp
    Status TEXT NOT NULL,           -- DAG run status
    StatusUpdateTs TEXT NOT NULL,   -- Status update timestamp (on first insert it's the same as InsertTs)
    Version TEXT NOT NULL           -- Scheduler Version
);

-- Table dagruntasks stores information about tasks state of DAG runs.
CREATE TABLE IF NOT EXISTS dagruntasks (
    DagId TEXT NOT NULL,            -- DAG ID
    ExecTs TEXT NOT NULL,           -- Execution timestamp
    TaskId TEXT NOT NULL,           -- Task ID
    InsertTs TEXT NOT NULL,         -- Insert timestamp
    Status TEXT NOT NULL,           -- DAG task execution status
    StatusUpdateTs TEXT NOT NULL,   -- Status update timestamp (on first insert it's the same as InsertTs)
    Version TEXT NOT NULL,          -- Scheduler version

    PRIMARY KEY (DagId, ExecTs, TaskId)
);

-- TODO: Think about caching latest dagrun into a separate table with PK(DagId)




