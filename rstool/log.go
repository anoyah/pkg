package rstool

import (
	"context"
	"log"
)

type Logger interface {
	Log(template string, args ...any)
	Printf(ctx context.Context, format string, v ...interface{})
}

type logging struct {
	*log.Logger
}

// Printf implements Logger.
// Subtle: this method shadows the method (*Logger).Printf of logging.Logger.
func (l *logging) Printf(_ context.Context, format string, v ...interface{}) {
	l.Log(format, v...)
}

// Log implements Logger.
func (l *logging) Log(template string, args ...any) {
	l.Logger.Printf(template, args...)
}

func defaultLogger() Logger {
	return &logging{
		Logger: log.Default(),
	}
}
