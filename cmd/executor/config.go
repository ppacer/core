package main

import (
	"flag"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type LoggerConfig struct {
	UseDebugLevel    bool
	UseConsoleWriter bool
}

type Config struct {
	Logger LoggerConfig
}

// Parse Config or fail.
func ParseConfig() Config {
	logDebugLevel := flag.Bool("logDebug", true,
		"Log events on at least debug level. Otherwise info level is assumed.")
	logUseConsoleWriter := flag.Bool("logConsole", true,
		"Use ConsoleWriter within zerolog - pretty but not efficient, mostly for development")
	flag.Parse()

	loggerCfg := LoggerConfig{
		UseDebugLevel:    *logDebugLevel,
		UseConsoleWriter: *logUseConsoleWriter,
	}
	return Config{Logger: loggerCfg}
}

func (c *Config) setupZerolog() {
	zerolog.DurationFieldUnit = time.Millisecond
	if c.Logger.UseDebugLevel {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	if c.Logger.UseConsoleWriter {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	} else {
		log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
		zerolog.TimeFieldFormat = time.RFC3339
	}
}
