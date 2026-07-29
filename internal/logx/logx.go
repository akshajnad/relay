// Package logx builds the slog loggers used by every Relay binary.
package logx

import (
	"io"
	"log/slog"
	"strings"
)

// New returns a logger writing format ("json" or "text") at level
// ("debug", "info", "warn", "error") to w.
func New(w io.Writer, level, format string) *slog.Logger {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		lv = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lv}
	if strings.EqualFold(format, "text") {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}
