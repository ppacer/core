package main

import (
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/dskrzypiec/scheduler/src/version"
)

type LoggerConfig struct {
	UseDebugLevel    bool
	UseConsoleWriter bool
}

type Config struct {
	AppVersion string
	Port       int
	Logger     LoggerConfig
}

// Parse Config or fail.
func ParseConfig() Config {
	logDebugLevel := flag.Bool("logDebug", false,
		"Log events on at least debug level. Otherwise info level is assumed.")
	logUseConsoleWriter := flag.Bool("logConsole", true,
		"Use ConsoleWriter - pretty but not efficient, mostly for development")
	port := flag.Int("port", 8080, "Port on which Scheduler is exposed")
	flag.Parse()

	loggerCfg := LoggerConfig{
		UseDebugLevel:    *logDebugLevel,
		UseConsoleWriter: *logUseConsoleWriter,
	}

	return Config{
		AppVersion: strings.TrimSpace(version.Version),
		Port:       *port,
		Logger:     loggerCfg,
	}
}

func (c *Config) setupLogger() {
	lvl := slog.LevelInfo
	if c.Logger.UseDebugLevel {
		lvl = slog.LevelDebug
	}
	var logger *slog.Logger
	if c.Logger.UseConsoleWriter {
		logger = slog.New(
			slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}),
		)
	} else {
		logger = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}),
		)
	}
	slog.SetDefault(logger)
}
