package streamflight

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Force endAll's worker path without sleeps: it must queue the held key's
// worker before it ends the free key. The worker must never run user hooks.
func TestEndAllLeavesHooksForTheStoppingCaller(t *testing.T) {
	x := require.New(t)
	upstreamErr := errors.New("upstream ended")
	var ended atomic.Int64
	var stops []string
	g := &Group[string, int]{
		Source: func(key string, _ Emitter[int]) (func() error, error) {
			return func() error { stops = append(stops, key); return nil }, nil
		},
		Hooks: Hooks[string, int]{Ended: func(string, error) {
			ended.Add(1)
			panic("ended hook")
		}},
	}
	held, err := g.Subscribe("held")
	x.NoError(err)
	free, err := g.Subscribe("free")
	x.NoError(err)
	f := g.flights["held"]
	f.quitOnce.Do(func() { f.endByItself(upstreamErr) })
	doomed := []*flight[string, int]{f, g.flights["free"]}
	g.mu.Lock()
	for _, flight := range doomed {
		g.beginStop(flight)
	}
	g.mu.Unlock()

	f.mu.Lock()
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		endAll(doomed)
	}()
	select {
	case <-free.Done():
	case <-time.After(5 * time.Second):
		f.mu.Unlock()
		t.Fatal("endAll did not end the free key while the other was held")
	}
	f.mu.Unlock()
	select {
	case p := <-done:
		x.Nil(p, "ending subscribers must not run a hook that can panic")
	case <-time.After(5 * time.Second):
		t.Fatal("endAll did not finish after the delivery released its lock")
	}
	x.Zero(ended.Load())
	x.ErrorIs(held.Err(), upstreamErr)
	x.ErrorIs(free.Err(), ErrGroupClosed)

	var errs []error
	x.PanicsWithValue("ended hook", func() { g.stopAll(doomed, &errs) })
	x.Equal(int64(1), ended.Load())
	x.Equal([]string{"held", "free"}, stops, "the hook must not skip either stop")
	x.Empty(g.flights)
	x.NoError(g.Close())
	x.NoError(held.Close())
	x.NoError(free.Close())
}

// An End can resume after endAll finished its subscribers but before stopAll
// reports it. Whichever caller reports first owns the hook, even if it panics.
func TestEndResumingAfterEndAllReportsOnlyOnce(t *testing.T) {
	for _, panics := range []bool{false, true} {
		name := "returns"
		if panics {
			name = "panics"
		}
		t.Run(name, func(t *testing.T) {
			x := require.New(t)
			var events []string
			g := &Group[string, int]{
				Source: func(string, Emitter[int]) (func() error, error) {
					return func() error { events = append(events, "stop"); return nil }, nil
				},
				Hooks: Hooks[string, int]{
					Ended: func(string, error) {
						events = append(events, "ended")
						if panics {
							panic("ended hook")
						}
					},
					Stopped: func(string, error) { events = append(events, "stopped") },
				},
			}
			s, err := g.Subscribe("k")
			x.NoError(err)
			f := g.flights["k"]
			first := errors.New("first end")
			f.quitOnce.Do(func() { f.endByItself(first) })
			g.mu.Lock()
			g.beginStop(f)
			g.mu.Unlock()
			endAll([]*flight[string, int]{f})
			x.Empty(events)
			if panics {
				x.PanicsWithValue("ended hook", func() { f.End(first) })
			} else {
				f.End(first)
			}
			f.End(errors.New("later end"))
			x.NoError(g.doStop(f, ErrGroupClosed))
			x.NoError(g.Close())
			x.NoError(s.Close())
			x.ErrorIs(s.Err(), first)
			x.Equal([]string{"ended", "stop", "stopped"}, events)
		})
	}
}

// A second panic during cleanup must not retry the stop or retain the key.
func TestEndedPanicWithPanickingCleanup(t *testing.T) {
	for _, where := range []string{"stop", "stopped hook"} {
		t.Run(where, func(t *testing.T) {
			x := require.New(t)
			stops := 0
			g := &Group[string, int]{
				Source: func(string, Emitter[int]) (func() error, error) {
					return func() error {
						stops++
						if where == "stop" {
							panic(where)
						}
						return nil
					}, nil
				},
				Hooks: Hooks[string, int]{
					Ended:   func(string, error) { panic("ended hook") },
					Stopped: func(string, error) { panic(where) },
				},
			}
			s, err := g.Subscribe("k")
			x.NoError(err)
			f := g.flights["k"]
			f.quitOnce.Do(func() { f.endByItself(errors.New("gone")) })
			x.PanicsWithValue(where, func() { _ = s.Close() })
			x.Equal(1, stops)
			x.Empty(g.flights)
			x.Nil(f.stop)
			x.NoError(g.Close())
		})
	}
}
