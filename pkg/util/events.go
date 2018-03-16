package util

import (
	"github.com/go-kit/kit/log"
)

var Eventer = New()

func New() log.Logger {
	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l = log.With(l, "ts", log.DefaultTimestampUTC)
	return l
}

func NewFilter(next log.Logger, options ...Option) log.Logger {
	l := &logger{
		next: next,
	}
	for _, option := range options {
		option(l)
	}
	return l
}
