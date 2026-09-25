package streamflight_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/heojeongbo/streamflight"
)

func Example() {
	var emit streamflight.Emitter[string]
	g := &streamflight.Group[string, string]{
		// Called once per upstream, by the first subscriber of the key.
		Source: func(key string, e streamflight.Emitter[string]) (func() error, error) {
			fmt.Println("open", key)
			emit = e
			return func() error {
				fmt.Println("stop", key)
				return nil
			}, nil
		},
	}

	a, _ := g.SubscribeFunc("temperature", func(v string) { fmt.Println("a:", v) })
	b, _ := g.SubscribeFunc("temperature", func(v string) { fmt.Println("b:", v) })

	emit.Emit("21.5")
	a.Close()
	emit.Emit("21.6")
	b.Close() // the last subscriber stops the upstream

	// Output:
	// open temperature
	// a: 21.5
	// b: 21.5
	// b: 21.6
	// stop temperature
}

// A config published only when it changes: a subscriber that joins late still
// receives the latest one.
func ExampleGroup_Subscribe() {
	var emit streamflight.Emitter[string]
	g := &streamflight.Group[string, string]{
		Source: func(key string, e streamflight.Emitter[string]) (func() error, error) {
			emit = e
			return nil, nil
		},
		Replay: 1,
	}

	first, _ := g.Subscribe("config")
	emit.Emit("config v1")
	fmt.Println("first:", <-first.C)

	late, _ := g.Subscribe("config")
	fmt.Println("late:", <-late.C)

	late.Close()
	first.Close()

	// Output:
	// first: config v1
	// late: config v1
}

func ExampleRun() {
	ready := make(chan struct{})
	g := &streamflight.Group[string, int]{
		Source: streamflight.Run(func(ctx context.Context, key string, e streamflight.Emitter[int]) error {
			<-ready
			for i := 1; i <= 3; i++ {
				e.Emit(i)
			}
			return nil // the upstream ends; subscribers see io.EOF
		}),
	}

	s, _ := g.Subscribe("counter", streamflight.WithBuffer(3))
	close(ready)
	for v := range s.C {
		fmt.Println(v)
	}
	fmt.Println(s.Err())
	s.Close()

	// Output:
	// 1
	// 2
	// 3
	// EOF
}

// A handler relaying one key to one client. send is whatever the protocol's
// write is: a gRPC stream's Send fits as a method value, and anything else is
// a closure.
func ExampleSubscription_Drain() {
	g := &streamflight.Group[string, string]{
		Replay: 1, // Poll's first tick can run before the opener is attached
		Source: streamflight.Poll(time.Hour,
			func(_ context.Context, key string, e streamflight.Emitter[string]) error {
				e.Emit("status of " + key)
				return nil
			}),
	}
	defer g.Close()

	sub, err := g.Subscribe("db1", streamflight.WithBuffer(8))
	if err != nil {
		return
	}
	defer sub.Close()

	// Stop after the first value, the way a client going away would.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fmt.Println(sub.Drain(ctx, func(v string) error {
		fmt.Println(v)
		cancel()
		return nil
	}))

	// Output:
	// status of db1
	// <nil>
}

// A thermostat's setpoint, read whenever the reader looks and read back after
// a write. One upstream shared by whoever wants it is a Group with one key.
func ExampleGroup_SubscribeLatest() {
	var device streamflight.Emitter[float64]
	g := &streamflight.Group[struct{}, float64]{
		Source: func(_ struct{}, e streamflight.Emitter[float64]) (func() error, error) {
			device = e
			e.Emit(21.5) // the current setpoint, read on subscribing
			return nil, nil
		},
	}
	defer g.Close()

	s, err := g.SubscribeLatest(struct{}{})
	if err != nil {
		return
	}
	defer s.Close()

	v, seen, _ := s.Latest() // as often as it likes: nothing is consumed
	fmt.Println("now:", v)

	device.Emit(22) // the write, reported back by the device
	v, _, _ = s.Wait(context.Background(), seen)
	fmt.Println("after:", v)

	// Output:
	// now: 21.5
	// after: 22
}

// Reading back a write that shows in the value itself. A report that was
// already on its way can still carry the old value, so wait for newer values
// until one has the new one.
func ExampleSubscription_Wait() {
	var device streamflight.Emitter[int]
	g := &streamflight.Group[string, int]{
		Source: func(_ string, e streamflight.Emitter[int]) (func() error, error) {
			device = e
			e.Emit(20)
			return nil, nil
		},
	}
	defer g.Close()

	s, err := g.SubscribeLatest("thermostat")
	if err != nil {
		return
	}
	defer s.Close()

	_, at, _ := s.Latest()
	set := func(want int) {
		go func() {
			device.Emit(20) // the report that was already on its way
			device.Emit(want)
		}()
	}
	set(22)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	v, ok := 0, false
	for {
		if v, at, ok = s.Wait(ctx, at); !ok || v == 22 {
			break
		}
	}
	fmt.Println(v, ok)

	// Output:
	// 22 true
}

// Setup a Source does per key, before the poll starts: here the interval comes
// from the key. A key the setup rejects fails Subscribe, rather than opening a
// stream that ends at once.
func ExamplePoll() {
	g := &streamflight.Group[string, string]{
		Replay: 1, // the first tick can run before the opener is attached
		Source: func(key string, e streamflight.Emitter[string]) (func() error, error) {
			host, every, _ := strings.Cut(key, "@")
			interval, err := time.ParseDuration(every)
			if err != nil {
				return nil, fmt.Errorf("bad key %q", key)
			}
			return streamflight.Poll(interval,
				func(_ context.Context, _ string, e streamflight.Emitter[string]) error {
					e.Emit("status of " + host)
					return nil
				})(key, e)
		},
	}
	defer g.Close()

	_, err := g.Subscribe("db1@often")
	fmt.Println(err)

	s, err := g.Subscribe("db1@1h")
	if err != nil {
		return
	}
	defer s.Close()
	fmt.Println(<-s.C)

	// Output:
	// bad key "db1@often"
	// status of db1
}
