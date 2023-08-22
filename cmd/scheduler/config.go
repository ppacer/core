package main

import (
	"flag"
	"go_shed/src/version"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
	port := flag.Int("port", 8080, "Port on which Scheduler is exposed")
	flag.Parse()

	return Config{
		AppVersion: strings.TrimSpace(version.Version),
		Port:       *port,
	}
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
