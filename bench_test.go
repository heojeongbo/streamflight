package streamflight_test

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"testing"

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
