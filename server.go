package syslog2sqlite

import (
	"github.com/ekanite/ekanite/input"
	"github.com/mysinmyc/gocommons/diagnostic"
)

type Server struct {
	collector    input.Collector
	eventStore   EventStore
	eventChannel chan *input.Event
}

const (
	backLog = 10000
)

func NewServer(pCollector input.Collector, pEventStore EventStore) (*Server, error) {

	vRis := &Server{
		collector:  pCollector,
		eventStore: pEventStore,
	}

	return vRis, nil
}

func (vSelf *Server) Start() error {

	vEventChannel := make(chan *input.Event, backLog)
	vSelf.eventChannel = vEventChannel

	if vError := vSelf.eventStore.Start(vEventChannel); vError != nil {
		return diagnostic.NewError("Error starting eventstore", vError)
	}

	if vError := vSelf.collector.Start(vEventChannel); vError != nil {
		return diagnostic.NewError("Error starting collector", vError)
	}

	return nil
}

func (vSelf *Server) Stop() error {

	if vError := vSelf.eventStore.Stop(); vError != nil {
		return diagnostic.NewError("Error stopping eventstore", vError)
	}

	close(vSelf.eventChannel)

	return nil
}
