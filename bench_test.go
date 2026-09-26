package streamflight_test

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heojeongbo/streamflight"
)

var fanouts = []int{1, 10, 100}

// openKey subscribes n times to one key with sub and returns the key's Emitter.
func openKey(b *testing.B, n int, sub func(g *streamflight.Group[string, int]) *streamflight.Subscription[int]) streamflight.Emitter[int] {
	b.Helper()
	var e streamflight.Emitter[int]
	g := &streamflight.Group[string, int]{
		Source: func(_ string, e_ streamflight.Emitter[int]) (func() error, error) {
			e = e_
			return nil, nil
		},
	}
	for range n {
		s := sub(g)
		b.Cleanup(func() { s.Close() })
	}
	return e
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// BenchmarkEmit is the cost of one value reaching n subscribers.
func BenchmarkEmit(b *testing.B) {
	for _, n := range fanouts {
		b.Run(fmt.Sprintf("func/subscribers=%d", n), func(b *testing.B) {
			e := openKey(b, n, func(g *streamflight.Group[string, int]) *streamflight.Subscription[int] {
				return must(g.SubscribeFunc("k", func(int) {}))
			})
			b.ReportAllocs()
			for b.Loop() {
				e.Emit(1)
			}
		})
		b.Run(fmt.Sprintf("chan/subscribers=%d", n), func(b *testing.B) {
			e := openKey(b, n, func(g *streamflight.Group[string, int]) *streamflight.Subscription[int] {
				s := must(g.Subscribe("k", streamflight.WithBuffer(64)))
				go func() {
					for range s.C {
					}
				}()
				return s
			})
			b.ReportAllocs()
			for b.Loop() {
				e.Emit(1)
			}
		})
		// Nobody reads: every value displaces the oldest queued one.
		b.Run(fmt.Sprintf("chan-full/subscribers=%d", n), func(b *testing.B) {
			e := openKey(b, n, func(g *streamflight.Group[string, int]) *streamflight.Subscription[int] {
				return must(g.Subscribe("k"))
			})
			b.ReportAllocs()
			for b.Loop() {
				e.Emit(1)
			}
		})
	}
}

// BenchmarkEmitLatest is one value reaching n subscribers that sample instead
// of being delivered to. The key stores it once however many are watching, so
// this should be flat in n.
func BenchmarkEmitLatest(b *testing.B) {
	for _, n := range fanouts {
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			e := openKey(b, n, func(g *streamflight.Group[string, int]) *streamflight.Subscription[int] {
				return must(g.SubscribeLatest("k"))
			})
			b.ReportAllocs()
			for b.Loop() {
				e.Emit(1)
			}
		})
	}
}

// BenchmarkLatest is a sampler reading the current value.
func BenchmarkLatest(b *testing.B) {
	var e streamflight.Emitter[int]
	g := &streamflight.Group[string, int]{
		Source: func(_ string, e_ streamflight.Emitter[int]) (func() error, error) {
			e = e_
			return nil, nil
		},
	}
	s := must(g.SubscribeLatest("k"))
	defer s.Close()
	e.Emit(1)

	b.ReportAllocs()
	for b.Loop() {
		s.Latest()
	}
}

// BenchmarkEmitParallel is one key emitted to from every P at once.
func BenchmarkEmitParallel(b *testing.B) {
	e := openKey(b, 10, func(g *streamflight.Group[string, int]) *streamflight.Subscription[int] {
		return must(g.SubscribeFunc("k", func(int) {}))
	})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Emit(1)
		}
	})
}

// BenchmarkSubscribe is joining and leaving a key that is already open.
func BenchmarkSubscribe(b *testing.B) {
	g := &streamflight.Group[string, int]{
		Source: func(string, streamflight.Emitter[int]) (func() error, error) { return nil, nil },
	}
	keep := must(g.SubscribeFunc("k", func(int) {}))
	defer keep.Close()

	b.Run("func", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			must(g.SubscribeFunc("k", func(int) {})).Close()
		}
	})
	b.Run("chan", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			must(g.Subscribe("k")).Close()
		}
	})
}

// BenchmarkOpenStop is the first subscriber opening a key and the last one
// stopping it.
func BenchmarkOpenStop(b *testing.B) {
	g := &streamflight.Group[string, int]{
		Source: func(string, streamflight.Emitter[int]) (func() error, error) {
			return func() error { return nil }, nil
		},
	}
	b.ReportAllocs()
	for b.Loop() {
		must(g.SubscribeFunc("k", func(int) {})).Close()
	}
}

// BenchmarkOpenStopParallel is many goroutines each opening and stopping a key
// of its own. Distinct keys share nothing, so this should scale with P.
func BenchmarkOpenStopParallel(b *testing.B) {
	g := &streamflight.Group[int, int]{
		Source: func(int, streamflight.Emitter[int]) (func() error, error) {
			return func() error { return nil }, nil
		},
	}
	var key atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		k := int(key.Add(1))
		for pb.Next() {
			must(g.SubscribeFunc(k, func(int) {})).Close()
		}
	})
}

// BenchmarkSlowSource is what an upstream that costs something to open does to
// the rest of the Group. Each goroutine opens a key of its own, so a Group that
// keeps keys independent finishes in about one delay regardless of how many
// goroutines there are.
func BenchmarkSlowSource(b *testing.B) {
	const delay = time.Millisecond
	for _, n := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			g := &streamflight.Group[int, int]{
				Source: func(int, streamflight.Emitter[int]) (func() error, error) {
					time.Sleep(delay)
					return func() error { return nil }, nil
				},
			}
			for b.Loop() {
				var wg sync.WaitGroup
				for k := range n {
					wg.Go(func() { must(g.SubscribeFunc(k, func(int) {})).Close() })
				}
				wg.Wait()
			}
		})
	}
}

// BenchmarkJoin is what a subscriber costs to join a key that is already open,
// for each way of catching it up.
func BenchmarkJoin(b *testing.B) {
	join := func(b *testing.B, g *streamflight.Group[string, int], emitted int) {
		b.Helper()
		var e streamflight.Emitter[int]
		g.Source = func(_ string, e_ streamflight.Emitter[int]) (func() error, error) {
			e = e_
			return nil, nil
		}
		keep := must(g.SubscribeFunc("k", func(int) {}))
		defer keep.Close()
		for i := range emitted {
			e.Emit(i) // what a joiner will be caught up on
		}
		b.ReportAllocs()
		for b.Loop() {
			must(g.Subscribe("k", streamflight.WithBuffer(8))).Close()
		}
	}
	b.Run("plain", func(b *testing.B) {
		join(b, &streamflight.Group[string, int]{}, 0)
	})
	b.Run("replay=8", func(b *testing.B) {
		join(b, &streamflight.Group[string, int]{Replay: 8}, 8)
	})
	b.Run("initial", func(b *testing.B) {
		join(b, &streamflight.Group[string, int]{
			Initial: func(_ string, send func(int)) { send(1) },
		}, 0)
	})
}

// BenchmarkOverflow is one value reaching a queue that is already full, under
// each policy that keeps the subscriber. Evict is not here: it closes the
// subscriber, so it happens once rather than per value, and Block cannot fill
// up at all while its subscriber reads.
func BenchmarkOverflow(b *testing.B) {
	for _, p := range []struct {
		name string
		o    streamflight.Overflow
	}{
		{"DropOldest", streamflight.DropOldest},
		{"DropNewest", streamflight.DropNewest},
	} {
		b.Run(p.name, func(b *testing.B) {
			var e streamflight.Emitter[int]
			g := &streamflight.Group[string, int]{
				Source: func(_ string, e_ streamflight.Emitter[int]) (func() error, error) {
					e = e_
					return nil, nil
				},
			}
			s := must(g.Subscribe("k", streamflight.WithOverflow(p.o)))
			defer s.Close()
			e.Emit(0) // fill the one-slot queue
			b.ReportAllocs()
			for b.Loop() {
				e.Emit(1)
			}
		})
	}
}

// BenchmarkLinger is churn on one key: a subscriber leaves and comes back
// before the upstream has finished lingering, so it is reused, not re-opened.
func BenchmarkLinger(b *testing.B) {
	g := &streamflight.Group[string, int]{Source: noopSource, Linger: time.Hour}
	defer g.Close()
	b.ReportAllocs()
	for b.Loop() {
		must(g.SubscribeFunc("k", func(int) {})).Close()
	}
}

func noopSource(string, streamflight.Emitter[int]) (func() error, error) {
	return nil, nil
}

// BenchmarkSharedVsDedicated is what sharing saves when every message costs
// something to produce, such as decoding it: a dedicated upstream per
// subscriber decodes each message n times, a shared one decodes it once.
func BenchmarkSharedVsDedicated(b *testing.B) {
	raw := bytes.Repeat([]byte{0xab}, 4<<10)
	decode := func() []byte {
		v := bytes.Clone(raw)
		crc32.ChecksumIEEE(v)
		return v
	}
	sink := func([]byte) {}

	for _, n := range fanouts {
		b.Run(fmt.Sprintf("dedicated/subscribers=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for range n {
					sink(decode())
				}
			}
		})
		b.Run(fmt.Sprintf("shared/subscribers=%d", n), func(b *testing.B) {
			var e streamflight.Emitter[[]byte]
			g := &streamflight.Group[string, []byte]{
				Source: func(_ string, e_ streamflight.Emitter[[]byte]) (func() error, error) {
					e = e_
					return nil, nil
				},
			}
			for range n {
				s := must(g.SubscribeFunc("k", sink))
				b.Cleanup(func() { s.Close() })
			}
			b.ReportAllocs()
			for b.Loop() {
				e.Emit(decode())
			}
		})
	}
}

// BenchmarkEvictAll is one Emit that evicts every subscriber of a key at once,
// the worst case for taking them out of the key's list.
func BenchmarkEvictAll(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				var e streamflight.Emitter[int]
				var subs []*streamflight.Subscription[int]
				g := &streamflight.Group[string, int]{
					Source: func(_ string, e_ streamflight.Emitter[int]) (func() error, error) {
						e = e_
						return nil, nil
					},
				}
				for range n {
					subs = append(subs, must(g.Subscribe("k", streamflight.WithOverflow(streamflight.Evict))))
				}
				e.Emit(0) // every queue is full
				b.StartTimer()

				e.Emit(1) // and every subscriber is evicted

				b.StopTimer()
				for _, s := range subs {
					s.Close()
				}
				b.StartTimer()
			}
		})
	}
}
