package streamflight_test

import (
	"context"
	"fmt"

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
