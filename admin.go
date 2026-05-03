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
	am.methodCounts[evt.Method]++
    am.consumerCounts[evt.Consumer]++
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
    // Capture initial counters (before any events for this stream are processed)
    am.mu.Lock()
    baseMethod := make(map[string]uint64)
    for k, v := range am.methodCounts {
        baseMethod[k] = v
    }
    baseConsumer := make(map[string]uint64)
    for k, v := range am.consumerCounts {
        baseConsumer[k] = v
    }
    am.mu.Unlock()

    ticker := time.NewTicker(time.Duration(n.IntervalSeconds) * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            am.mu.Lock()
            currentStat := &Stat{
                Timestamp:  time.Now().Unix(),
                ByMethod:   make(map[string]uint64),
                ByConsumer: make(map[string]uint64),
            }
            for k, v := range am.methodCounts {
                if v > baseMethod[k] {
                    currentStat.ByMethod[k] = v - baseMethod[k]
                }
            }
            for k, v := range am.consumerCounts {
                if v > baseConsumer[k] {
                    currentStat.ByConsumer[k] = v - baseConsumer[k]
                }
            }
            // Update base for next interval
            baseMethod = make(map[string]uint64)
            for k, v := range am.methodCounts {
                baseMethod[k] = v
            }
            baseConsumer = make(map[string]uint64)
            for k, v := range am.consumerCounts {
                baseConsumer[k] = v
            }
            am.mu.Unlock()

            if err := stream.Send(currentStat); err != nil {
                return err
            }
        case <-stream.Context().Done():
            return nil
        }
    }
}