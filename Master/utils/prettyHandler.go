package utils

import (
	"context"
	"github.com/fatih/color"
	"io"
	"log/slog"
	"sync"
)

type PrettyHandler struct {
	slog.Handler
	out io.Writer
	mu  *sync.Mutex
}

func (h *PrettyHandler) Handle(_ context.Context, r slog.Record) error {
	level := r.Level.String()

	switch r.Level {
	case slog.LevelDebug:
		level = color.MagentaString(level)
	case slog.LevelInfo:
		level = color.BlueString(level)
	case slog.LevelWarn:
		level = color.YellowString(level)
	case slog.LevelError:
		level = color.RedString(level)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.out.Write([]byte(r.Time.Format("2006/01/02 15:04:05") + " " + level + " " + r.Message + "\n"))
	if err != nil {
		return err
	}
	return nil
}

func NewPrettyHandler(out io.Writer, opts slog.HandlerOptions) *PrettyHandler {
	h := &PrettyHandler{
		Handler: slog.NewTextHandler(out, &opts),
		out:     out,
		mu:      &sync.Mutex{},
	}
	return h
}
