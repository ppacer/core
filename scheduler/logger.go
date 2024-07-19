package scheduler

import (
	"log/slog"
	"os"
)

// Name for environment variable for setting ppacer default logger severity
// level.
const PPACER_ENV_LOG_LEVEL = "PPACER_LOG_LEVEL"

func defaultLogger() *slog.Logger {
	level := os.Getenv(PPACER_ENV_LOG_LEVEL)
	var logLevel slog.Level
	switch level {
	case "DEBUG":
		logLevel = slog.LevelDebug
	case "INFO":
		logLevel = slog.LevelInfo
	case "WARN":
		logLevel = slog.LevelWarn
	case "ERROR":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	opts := slog.HandlerOptions{Level: logLevel}
	return slog.New(slog.NewTextHandler(os.Stdout, &opts))
}
