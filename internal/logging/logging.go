// Package logging builds the structured JSON logger shared by all services.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON logger writing to stdout at the given level
// ("debug", "info", "warn" or "error"; unknown values fall back to info).
func New(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
