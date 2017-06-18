package syslog2sqlite

import (
	"github.com/ekanite/ekanite/input"
)

type EventStore interface {
	Start(chan *input.Event) error
	Stop() error
}
