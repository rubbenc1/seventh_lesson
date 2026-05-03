package main

import (
	"sync"
	"time"

	"google.golang.org/grpc"
)

type AdminManager struct {
	mu         sync.Mutex
	subsribers map[chan *Event]struct{}
	methodCounts map[string]uint64
	consumerCounts map[string]uint64
	UnimplementedAdminServer
}

func NewAdminManager() *AdminManager {
	return &AdminManager{
		mu:         sync.Mutex{},
		subsribers: map[chan *Event]struct{}{},
		methodCounts: map[string]uint64{},
		consumerCounts: map[string]uint64{},
	}
}

func (am *AdminManager) Publish(evt *Event) {
	am.mu.Lock()
	defer am.mu.Unlock()
	for ch := range am.subsribers {
		select {
		case ch <- evt:
		default:
		}
	}
}

func (am *AdminManager) Logging(n *Nothing, stream grpc.ServerStreamingServer[Event]) error {
	eventCh := make(chan *Event, 10)
	am.mu.Lock()
	am.subsribers[eventCh] = struct{}{}
	am.mu.Unlock()
	defer func() {
		am.mu.Lock()
		delete(am.subsribers, eventCh)
		am.mu.Unlock()
		close(eventCh)
	}()

	for {
		select {
		case evt := <-eventCh:
			err := stream.Send(evt)
			if err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (am *AdminManager) Statistics(n *StatInterval, stream grpc.ServerStreamingServer[Stat]) error {
	ticker:=time.NewTicker(time.Duration(n.IntervalSeconds)*time.Second)
	defer ticker.Stop()
	stat:=&Stat{
		ByMethod: am.methodCounts,
		ByConsumer: am.consumerCounts,
	}
	for {
		select {
		case <-ticker.C:
			err:=stream.Send(stat)
			if err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}

	}
}