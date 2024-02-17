package dag

import (
	"log/slog"
)

type TaskLoggerFunc func(RunInfo) *slog.Logger
