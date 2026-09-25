package streamflight_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/heojeongbo/streamflight"
)

// recorder is a Source that logs every open and stop and keeps the Emitter of
// each open key.
type recorder struct {
	mu       sync.Mutex
	log      []string
	emitters map[string]streamflight.Emitter[int]

	openErr error
	stopErr error
	nilStop bool
}

func newRecorder() *recorder {
	return &recorder{emitters: map[string]streamflight.Emitter[int]{}}
}

func (r *recorder) Source(key string, e streamflight.Emitter[int]) (func() error, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.log = append(r.log, "open "+key)
	if r.openErr != nil {
		return nil, r.openErr
	}
	r.emitters[key] = e
	if r.nilStop {
		return nil, nil
	}
	return func() error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.log = append(r.log, "stop "+key)
		return r.stopErr
	}, nil
}

func (r *recorder) emitter(key string) streamflight.Emitter[int] {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.emitters[key]
}

func (r *recorder) emit(key string, vs ...int) int {
	n := 0
	for _, v := range vs {
		n = r.emitter(key).Emit(v)
	}
	return n
}

func (r *recorder) Log() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.log...)
}

func (r *recorder) set(f func(r *recorder)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f(r)
}

// collector is a SubscribeFunc function that keeps what it receives.
type collector struct {
	mu sync.Mutex
	vs []int
}

func (c *collector) Add(v int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.vs = append(c.vs, v)
}

func (c *collector) Values() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.vs...)
}

// drain reads what is queued on a closed or idle channel without waiting.
func drain(c <-chan int) []int {
	vs := []int{}
	for {
		select {
		case v, ok := <-c:
			if !ok {
				return vs
			}
			vs = append(vs, v)
		default:
			return vs
		}
	}
}

func closed(c <-chan int) bool {
	select {
	case _, ok := <-c:
		return !ok
	default:
		return false
	}
}

func TestSharing(t *testing.T) {
	t.Run("subscribers of a key share one upstream", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		var a, b collector
		sa, err := g.SubscribeFunc("k", a.Add)
		x.NoError(err)
		sb, err := g.SubscribeFunc("k", b.Add)
		x.NoError(err)

		x.Equal(2, r.emit("k", 1))
		x.Equal([]int{1}, a.Values())
		x.Equal([]int{1}, b.Values())
		x.Equal([]string{"open k"}, r.Log())

		x.NoError(sa.Close())
		x.Equal([]string{"open k"}, r.Log(), "a subscriber remains")
		x.NoError(sb.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("concurrent subscribers open the upstream once", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		const n = 64
		subs := make([]*streamflight.Subscription[int], n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range subs {
			wg.Go(func() { subs[i], errs[i] = g.Subscribe("k") })
		}
		wg.Wait()
		for _, err := range errs {
			x.NoError(err)
		}
		x.Equal([]string{"open k"}, r.Log())

		x.Equal(n, r.emit("k", 7))
		for _, s := range subs {
			x.Equal(7, <-s.C)
			x.NoError(s.Close())
		}
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("keys are independent", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		var a, b collector
		sa, err := g.SubscribeFunc("a", a.Add)
		x.NoError(err)
		sb, err := g.SubscribeFunc("b", b.Add)
		x.NoError(err)

		r.emit("a", 1)
		r.emit("b", 2)
		x.Equal([]int{1}, a.Values())
		x.Equal([]int{2}, b.Values())

		x.NoError(sa.Close())
		x.Equal([]string{"open a", "open b", "stop a"}, r.Log())
		x.NoError(sb.Close())
	})
	t.Run("the key is forgotten once stopped", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k")
		x.NoError(err)
		x.NoError(s.Close())

		s, err = g.Subscribe("k")
		x.NoError(err)
		x.NoError(s.Close())
		x.Equal([]string{"open k", "stop k", "open k", "stop k"}, r.Log())
	})
	t.Run("a nil stop means nothing to stop", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		r.nilStop = true
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k")
		x.NoError(err)
		x.NoError(s.Close())
		x.Equal([]string{"open k"}, r.Log())
	})
}

func TestOpenAndStopErrors(t *testing.T) {
	t.Run("an open error is returned and nothing is left behind", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		r := newRecorder()
		r.openErr = boom
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.SubscribeFunc("k", func(int) { x.Fail("delivered to a failed subscribe") })
		x.ErrorIs(err, boom)
		x.Nil(s)

		s, err = g.Subscribe("k")
		x.ErrorIs(err, boom)
		x.Nil(s)

		r.set(func(r *recorder) { r.openErr = nil })
		s, err = g.Subscribe("k")
		x.NoError(err)
		x.NoError(s.Close())
		x.Equal([]string{"open k", "open k", "open k", "stop k"}, r.Log())
	})
	t.Run("the last close reports the stop error, every time", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		r := newRecorder()
		r.stopErr = boom
		g := &streamflight.Group[string, int]{Source: r.Source}

		a, err := g.Subscribe("k")
		x.NoError(err)
		b, err := g.Subscribe("k")
		x.NoError(err)

		x.NoError(a.Close())
		x.NoError(a.Close(), "idempotent: releases one reference only")
		x.ErrorIs(b.Close(), boom)
		x.ErrorIs(b.Close(), boom)
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
}

func TestSubscription(t *testing.T) {
	t.Run("Err says why it ended", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k")
		x.NoError(err)
		x.NoError(s.Err())
		select {
		case <-s.Done():
			x.Fail("done while live")
		default:
		}

		x.NoError(s.Close())
		<-s.Done()
		x.ErrorIs(s.Err(), streamflight.ErrClosed)
		x.True(closed(s.C))
	})
	t.Run("a function subscription has no channel", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{Source: newRecorder().Source}

		s, err := g.SubscribeFunc("k", func(int) {})
		x.NoError(err)
		x.Nil(s.C)
		x.NoError(s.Close())
		x.ErrorIs(s.Err(), streamflight.ErrClosed)
	})
	t.Run("the queue holds at least one value", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{Source: newRecorder().Source}

		s, err := g.Subscribe("k", streamflight.WithBuffer(0))
		x.NoError(err)
		x.Equal(1, cap(s.C))
		x.NoError(s.Close())

		s, err = g.Subscribe("k", streamflight.WithBuffer(8))
		x.NoError(err)
		x.Equal(8, cap(s.C))
		x.NoError(s.Close())
	})
	t.Run("why it ended is visible as soon as the channel closes", func(t *testing.T) {
		// The documented idiom is to range over C and then ask Err why it
		// ended, so the reason has to be readable the moment C closes.
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k", streamflight.WithBuffer(4))
		x.NoError(err)
		r.emit("k", 1, 2)

		var got error
		var wg sync.WaitGroup
		wg.Go(func() {
			for range s.C {
			}
			got = s.Err()
		})
		r.emitter("k").End(io.EOF)
		wg.Wait()

		x.ErrorIs(got, io.EOF, "the reason was lost to a reader woken by the close")
		x.NoError(s.Close())
	})
	t.Run("what was queued is still read after close", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k", streamflight.WithBuffer(4))
		x.NoError(err)
		r.emit("k", 1, 2)
		x.NoError(s.Close())

		x.Equal([]int{1, 2}, drain(s.C))
		x.True(closed(s.C))
	})
}

func TestDrain(t *testing.T) {
	t.Run("it sends every value until the context is done", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k", streamflight.WithBuffer(8))
		x.NoError(err)
		defer s.Close()
		r.emit("k", 1, 2, 3)

		ctx, cancel := context.WithCancel(context.Background())
		var got []int
		x.NoError(s.Drain(ctx, func(v int) error {
			got = append(got, v)
			if len(got) == 3 {
				cancel() // the client went away: a clean end, not a failure
			}
			return nil
		}))
		x.Equal([]int{1, 2, 3}, got)
	})
	t.Run("an upstream that ends cleanly is not a failure", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k", streamflight.WithBuffer(8))
		x.NoError(err)
		defer s.Close()
		r.emit("k", 1)
		r.emitter("k").End(nil)

		var got []int
		x.NoError(s.Drain(context.Background(), func(v int) error {
			got = append(got, v)
			return nil
		}))
		x.Equal([]int{1}, got, "what was queued is sent before the end")
	})
	t.Run("it returns why the subscription ended", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k")
		x.NoError(err)
		defer s.Close()
		r.emitter("k").End(boom)

		x.ErrorIs(s.Drain(context.Background(), func(int) error { return nil }), boom)
	})
	t.Run("it returns what send returned", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k", streamflight.WithBuffer(8))
		x.NoError(err)
		defer s.Close()
		r.emit("k", 1, 2)

		n := 0
		x.ErrorIs(s.Drain(context.Background(), func(int) error { n++; return boom }), boom)
		x.Equal(1, n, "it stops on the first failure")
	})
	t.Run("a function subscription has nothing to drain", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{Source: newRecorder().Source}

		s, err := g.SubscribeFunc("k", func(int) {})
		x.NoError(err)
		defer s.Close()

		x.PanicsWithValue("streamflight: Drain on a subscription with no channel", func() {
			s.Drain(context.Background(), func(int) error { return nil })
		})
	})
}

func TestSubscribeLatest(t *testing.T) {
	t.Run("a sampler reads the newest value as often as it likes", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer s.Close()
		x.Nil(s.C, "nothing is queued")

		_, _, ok := s.Latest()
		x.False(ok, "nothing has been emitted yet")

		r.emit("k", 1, 2, 3)
		for range 3 {
			// The read is not destructive, which is the whole point: a channel
			// would have answered once and then run dry.
			v, at, ok := s.Latest()
			x.True(ok)
			x.Equal(3, v)
			x.False(at.IsZero())
		}
	})
	t.Run("every sampler of a key shares one stored value", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		a, err := g.SubscribeLatest("k")
		x.NoError(err)
		b, err := g.SubscribeLatest("k")
		x.NoError(err)
		x.Equal([]string{"open k"}, r.Log(), "one upstream, as ever")

		x.Equal(2, r.emit("k", 7), "both count as having taken it")
		av, aat, _ := a.Latest()
		bv, bat, _ := b.Latest()
		x.Equal(7, av)
		x.Equal(av, bv)
		x.Equal(aat, bat, "the same stored value, not a copy each")

		x.NoError(a.Close())
		x.Equal([]string{"open k"}, r.Log(), "b still holds it")
		x.NoError(b.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("a sampler joining later reads what is already there", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		first, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer first.Close()
		r.emit("k", 5)

		late, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer late.Close()
		v, _, ok := late.Latest()
		x.True(ok)
		x.Equal(5, v, "the key was already keeping it")
	})
	t.Run("a sampler that opens the key keeps what its Source emits while opening", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{
			Source: func(_ string, e streamflight.Emitter[int]) (func() error, error) {
				// The current state, read while subscribing to its changes.
				x.Equal(0, e.Emit(42), "nobody is attached yet")
				return nil, nil
			},
		}

		s, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer s.Close()
		v, _, ok := s.Latest()
		x.True(ok, "kept without Replay, which does not reach a sampler anyway")
		x.Equal(42, v)
	})
	t.Run("Poll's first tick reaches a sampler that opened the key", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			ticked := make(chan struct{}, 1)
			poll := streamflight.Poll(time.Hour,
				func(_ context.Context, _ string, e streamflight.Emitter[int]) error {
					e.Emit(7)
					select {
					case ticked <- struct{}{}:
					default:
					}
					return nil
				})
			g := &streamflight.Group[string, int]{
				// Hold the open until the first tick has landed: the schedule
				// that would lose it if the key kept values only from attach.
				Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
					stop, err := poll(key, e)
					<-ticked
					return stop, err
				},
			}

			s, err := g.SubscribeLatest("k")
			x.NoError(err)
			v, _, ok := s.Latest()
			x.True(ok, "on open, not after one interval")
			x.Equal(7, v)
			x.NoError(s.Close())
		})
	})
	t.Run("a key nobody samples never reads the clock", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var reads atomic.Int64
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Now: func() time.Time {
				reads.Add(1)
				return time.Now()
			},
		}

		ch, err := g.Subscribe("k")
		x.NoError(err)
		defer ch.Close()
		r.emit("k", 1)
		x.Zero(reads.Load(), "a channel subscriber opened it")

		s, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer s.Close()
		r.emit("k", 2)
		x.Equal(int64(1), reads.Load(), "from the first sampler on")
	})
	t.Run("a sampler does not hold up the values a channel gets", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		sampler, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer sampler.Close()
		ch, err := g.Subscribe("k", streamflight.WithBuffer(4))
		x.NoError(err)
		defer ch.Close()

		r.emit("k", 1, 2)
		x.Equal([]int{1, 2}, drain(ch.C), "the queue is unaffected")
		v, _, _ := sampler.Latest()
		x.Equal(2, v)
	})
	t.Run("it ends like any other subscription", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.SubscribeLatest("k")
		x.NoError(err)
		r.emit("k", 1)
		r.emitter("k").End(boom)

		<-s.Done()
		x.ErrorIs(s.Err(), boom)
		v, _, ok := s.Latest()
		x.True(ok)
		x.Equal(1, v, "the last value it saw is still readable")
		x.NoError(s.Close())
	})
	t.Run("Latest is only for a subscription that is not delivered to", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{Source: newRecorder().Source}

		ch, err := g.Subscribe("k")
		x.NoError(err)
		defer ch.Close()
		fn, err := g.SubscribeFunc("k", func(int) {})
		x.NoError(err)
		defer fn.Close()

		const msg = "streamflight: Latest on a subscription that is delivered to"
		x.PanicsWithValue(msg, func() { ch.Latest() })
		x.PanicsWithValue(msg, func() { fn.Latest() })
	})
	t.Run("a value is the key's newest before any subscriber is delivered to", func(t *testing.T) {
		// One subscription carries the state, another the edge. A hook woken by
		// a value must read that value, not the one before it, or a consumer
		// that diffs against Latest when it wakes suppresses the transition.
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		sampler, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer sampler.Close()

		var sawWhenWoken []int
		edge, err := g.SubscribeFunc("k", func(int) {
			v, _, ok := sampler.Latest()
			x.True(ok, "the value that woke this hook is not stored yet")
			sawWhenWoken = append(sawWhenWoken, v)
		})
		x.NoError(err)
		defer edge.Close()

		r.emit("k", 1, 2, 3)
		x.Equal([]int{1, 2, 3}, sawWhenWoken, "each wake-up read its own value")
	})
	t.Run("Now is where the arrival time comes from", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		stale := time.Now().Add(-time.Hour)
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Now:    func() time.Time { return stale },
		}

		s, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer s.Close()

		r.emit("k", 1)
		_, at, ok := s.Latest()
		x.True(ok)
		x.Equal(stale, at, "a test can age a value")

		// Still strictly increasing, so waiting past one ends even on a clock
		// that never moves.
		r.emit("k", 2)
		_, next, _ := s.Latest()
		x.True(next.After(at))
	})
	t.Run("Wait blocks until a value newer than the one already there", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			g := &streamflight.Group[string, int]{Source: r.Source}

			s, err := g.SubscribeLatest("k")
			x.NoError(err)
			defer s.Close()

			r.emit("k", 1)
			_, at, ok := s.Latest()
			x.True(ok)

			// Reading back after a write: the value that is there is the one
			// the write has not reached yet, so waiting past it is the point.
			got := make(chan int, 1)
			go func() {
				v, _, ok := s.Wait(t.Context(), at)
				x.True(ok)
				got <- v
			}()
			synctest.Wait()
			x.Empty(got, "still the old value")

			r.emit("k", 2)
			x.Equal(2, <-got)
		})
	})
	t.Run("Wait gives up with its context and with the subscription", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			g := &streamflight.Group[string, int]{Source: r.Source}

			s, err := g.SubscribeLatest("k")
			x.NoError(err)
			defer s.Close()

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan bool, 1)
			go func() { _, _, ok := s.Wait(ctx, time.Time{}); done <- ok }()
			synctest.Wait()
			cancel()
			x.False(<-done, "the caller gave up")

			ended := make(chan bool, 1)
			go func() { _, _, ok := s.Wait(t.Context(), time.Time{}); ended <- ok }()
			synctest.Wait()
			r.emitter("k").End(nil)
			x.False(<-ended, "the upstream ended")
		})
	})
	t.Run("Wait is only for a subscription that is not delivered to", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{Source: newRecorder().Source}

		ch, err := g.Subscribe("k")
		x.NoError(err)
		defer ch.Close()

		x.PanicsWithValue("streamflight: Wait on a subscription that is delivered to", func() {
			ch.Wait(context.Background(), time.Time{})
		})
	})
	t.Run("an open error is returned, as for any subscribe", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		r := newRecorder()
		r.openErr = boom
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.SubscribeLatest("k")
		x.ErrorIs(err, boom)
		x.Nil(s)
	})
	t.Run("a sampler has no channel to drain", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{Source: newRecorder().Source}

		s, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer s.Close()

		x.PanicsWithValue("streamflight: Drain on a subscription with no channel", func() {
			s.Drain(context.Background(), func(int) error { return nil })
		})
	})
}

func TestOverflow(t *testing.T) {
	t.Run("DropOldest keeps the newest values", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var dropped []int
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Dropped: func(_ string, v int) { dropped = append(dropped, v) },
			},
		}

		s, err := g.Subscribe("k", streamflight.WithBuffer(2))
		x.NoError(err)
		for v := 1; v <= 5; v++ {
			x.Equal(1, r.emit("k", v), "the arriving value is accepted")
		}
		x.Equal([]int{4, 5}, drain(s.C))
		x.Equal(uint64(3), s.Dropped())
		x.Equal([]int{1, 2, 3}, dropped, "the displaced values")
		x.NoError(s.Close())
	})
	t.Run("DropNewest keeps the oldest values", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var dropped []int
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Dropped: func(_ string, v int) { dropped = append(dropped, v) },
			},
		}

		s, err := g.Subscribe("k", streamflight.WithBuffer(2), streamflight.WithOverflow(streamflight.DropNewest))
		x.NoError(err)
		x.Equal(1, r.emit("k", 1))
		x.Equal(1, r.emit("k", 2))
		x.Equal(0, r.emit("k", 3), "the arriving value is refused")
		x.Equal(0, r.emit("k", 4))
		x.Equal([]int{1, 2}, drain(s.C))
		x.Equal(uint64(2), s.Dropped())
		x.Equal([]int{3, 4}, dropped)
		x.NoError(s.Close())
	})
	t.Run("Evict cuts off a subscriber that falls behind, and only it", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		slow, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Evict))
		x.NoError(err)
		var fast collector
		sf, err := g.SubscribeFunc("k", fast.Add)
		x.NoError(err)

		x.Equal(2, r.emit("k", 1))
		x.Equal(1, r.emit("k", 2), "slow is full and is evicted")
		x.Equal(1, r.emit("k", 3))

		<-slow.Done()
		x.ErrorIs(slow.Err(), streamflight.ErrEvicted)
		x.Equal([]int{1}, drain(slow.C))
		x.Equal([]int{1, 2, 3}, fast.Values())

		x.NoError(sf.Close())
		x.Equal([]string{"open k"}, r.Log(), "an evicted subscription still holds the upstream")
		x.NoError(slow.Close())
		x.ErrorIs(slow.Err(), streamflight.ErrEvicted, "close does not rewrite why it ended")
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("Block waits for the subscriber to make room", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			g := &streamflight.Group[string, int]{Source: r.Source}

			s, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
			x.NoError(err)
			x.Equal(1, r.emit("k", 1))

			var n atomic.Int64
			go func() { n.Store(int64(r.emit("k", 2))) }()
			synctest.Wait()
			x.Equal(int64(0), n.Load(), "still waiting")

			x.Equal(1, <-s.C)
			synctest.Wait()
			x.Equal(int64(1), n.Load())
			x.Equal(2, <-s.C)
			x.NoError(s.Close())
		})
	})
	t.Run("Block is released by closing the subscription", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			g := &streamflight.Group[string, int]{Source: r.Source}

			s, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
			x.NoError(err)
			r.emit("k", 1)

			n := make(chan int)
			go func() { n <- r.emit("k", 2) }()
			synctest.Wait()

			x.NoError(s.Close())
			x.Equal(0, <-n)
			x.Equal([]string{"open k", "stop k"}, r.Log())
		})
	})
	t.Run("Block is released by closing the group", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			g := &streamflight.Group[string, int]{Source: r.Source}

			s, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
			x.NoError(err)
			r.emit("k", 1)

			n := make(chan int)
			go func() { n <- r.emit("k", 2) }()
			synctest.Wait()

			x.NoError(g.Close())
			x.Equal(0, <-n)
			x.ErrorIs(s.Err(), streamflight.ErrGroupClosed)
			x.NoError(s.Close())
		})
	})
	t.Run("Block is released by the upstream ending", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			g := &streamflight.Group[string, int]{Source: r.Source}

			s, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
			x.NoError(err)
			r.emit("k", 1)

			n := make(chan int)
			go func() { n <- r.emit("k", 2) }()
			synctest.Wait()

			r.emitter("k").End(nil)
			x.Equal(0, <-n)
			x.ErrorIs(s.Err(), io.EOF)
			x.NoError(s.Close())
		})
	})
}

func TestReplay(t *testing.T) {
	t.Run("a joining subscriber first receives the latest values, oldest first", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source, Replay: 2}

		first, err := g.Subscribe("k", streamflight.WithBuffer(8))
		x.NoError(err)
		x.Empty(drain(first.C), "nothing to replay yet")
		r.emit("k", 1, 2, 3)

		var late collector
		sl, err := g.SubscribeFunc("k", late.Add)
		x.NoError(err)
		x.Equal([]int{2, 3}, late.Values())

		r.emit("k", 4)
		x.Equal([]int{2, 3, 4}, late.Values())
		x.Equal([]int{1, 2, 3, 4}, drain(first.C), "replay reaches only the joiner")

		x.NoError(first.Close())
		x.NoError(sl.Close())
	})
	t.Run("a short queue keeps the newest replayed values without blocking", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source, Replay: 3}

		keep, err := g.SubscribeFunc("k", func(int) {})
		x.NoError(err)
		r.emit("k", 1, 2, 3)

		s, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
		x.NoError(err)
		x.Equal([]int{3}, drain(s.C))

		x.NoError(s.Close())
		x.NoError(keep.Close())
	})
	t.Run("a value emitted while opening is replayed to the opener", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{
			Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
				x.Equal(0, e.Emit(42), "no subscriber is attached yet")
				return nil, nil
			},
			Replay: 1,
		}

		s, err := g.Subscribe("k")
		x.NoError(err)
		x.Equal(42, <-s.C)
		x.NoError(s.Close())
	})
	t.Run("nothing is replayed without Replay", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		keep, err := g.SubscribeFunc("k", func(int) {})
		x.NoError(err)
		r.emit("k", 1)

		s, err := g.Subscribe("k")
		x.NoError(err)
		x.Empty(drain(s.C))

		x.NoError(s.Close())
		x.NoError(keep.Close())
	})
}

func TestReplayFor(t *testing.T) {
	t.Run("a key's replay replaces the Group's", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Replay: 1, // replaced for every key, so never used
			ReplayFor: func(key string) int {
				if key == "state" {
					return 2
				}
				return 0 // an event replayed is an old event delivered as new
			},
		}

		state, err := g.SubscribeFunc("state", func(int) {})
		x.NoError(err)
		events, err := g.SubscribeFunc("events", func(int) {})
		x.NoError(err)
		r.emit("state", 1, 2, 3)
		r.emit("events", 7, 8)

		var late, none collector
		sl, err := g.SubscribeFunc("state", late.Add)
		x.NoError(err)
		x.Equal([]int{2, 3}, late.Values(), "the last two, on one Group")

		sn, err := g.SubscribeFunc("events", none.Add)
		x.NoError(err)
		x.Empty(none.Values(), "and nothing at all on another")

		for _, s := range []*streamflight.Subscription[int]{state, events, sl, sn} {
			x.NoError(s.Close())
		}
	})
	t.Run("it runs once per upstream, outside the Group lock", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var keys []string
		g := &streamflight.Group[string, int]{Source: r.Source}
		g.ReplayFor = func(key string) int {
			keys = append(keys, key)
			// Free to use the Group: no lock of its own is held.
			if key == "a" {
				s, err := g.Subscribe("b")
				x.NoError(err)
				x.NoError(s.Close())
			}
			return 1
		}

		a, err := g.Subscribe("a")
		x.NoError(err)
		b, err := g.Subscribe("a")
		x.NoError(err)
		x.Equal([]string{"a", "b"}, keys, "once for the upstream, not once per subscriber")

		x.NoError(a.Close())
		x.NoError(b.Close())
		c, err := g.Subscribe("a")
		x.NoError(err)
		x.Equal([]string{"a", "b", "a", "b"}, keys, "and again for a fresh upstream")
		x.NoError(c.Close())
	})
}

func TestInitial(t *testing.T) {
	t.Run("a joining subscriber alone receives what Initial sends, after the replay", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		state := 0
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Replay: 1,
			Initial: func(key string, send func(int)) {
				x.Equal("k", key)
				send(100 + state)
			},
		}

		var first collector
		sf, err := g.SubscribeFunc("k", first.Add)
		x.NoError(err)
		x.Equal([]int{100}, first.Values())

		state = 1
		r.emit("k", 1)

		var late collector
		sl, err := g.SubscribeFunc("k", late.Add)
		x.NoError(err)
		x.Equal([]int{1, 101}, late.Values())
		x.Equal([]int{100, 1}, first.Values())

		x.NoError(sf.Close())
		x.NoError(sl.Close())
	})
}

func TestMisuse(t *testing.T) {
	t.Run("subscribing without a Source panics", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{}

		x.PanicsWithValue("streamflight: Group.Source is nil", func() {
			g.Subscribe("k")
		})
		x.PanicsWithValue("streamflight: Group.Source is nil", func() {
			g.SubscribeFunc("k", func(int) {})
		})
	})
	t.Run("retaining the send of Initial panics", func(t *testing.T) {
		x := require.New(t)
		var escaped func(int)
		g := &streamflight.Group[string, int]{
			Source:  newRecorder().Source,
			Initial: func(_ string, send func(int)) { escaped = send },
		}

		s, err := g.Subscribe("k")
		x.NoError(err)
		x.PanicsWithValue("streamflight: Group.Initial called send after returning", func() {
			escaped(1)
		})
		x.NoError(s.Close())
	})
}

func TestLinger(t *testing.T) {
	t.Run("the upstream stops once it has lingered", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			g := &streamflight.Group[string, int]{Source: r.Source, Linger: time.Second}

			s, err := g.Subscribe("k")
			x.NoError(err)
			x.NoError(s.Close())
			x.Equal([]string{"open k"}, r.Log())

			time.Sleep(time.Second)
			synctest.Wait()
			x.Equal([]string{"open k", "stop k"}, r.Log())
		})
	})
	t.Run("a subscriber that comes back in time reuses the upstream", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			g := &streamflight.Group[string, int]{Source: r.Source, Linger: time.Second, Replay: 1}

			s, err := g.Subscribe("k")
			x.NoError(err)
			x.NoError(s.Close())

			time.Sleep(time.Second / 2)
			r.emit("k", 7)
			s, err = g.Subscribe("k")
			x.NoError(err)
			x.Equal(7, <-s.C, "a value emitted while lingering is replayed")

			time.Sleep(2 * time.Second)
			synctest.Wait()
			x.Equal([]string{"open k"}, r.Log(), "the linger was disarmed")

			x.NoError(s.Close())
			time.Sleep(time.Second)
			synctest.Wait()
			x.Equal([]string{"open k", "stop k"}, r.Log())
		})
	})
	t.Run("closing the group stops a lingering upstream", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			g := &streamflight.Group[string, int]{Source: r.Source, Linger: time.Second}

			s, err := g.Subscribe("k")
			x.NoError(err)
			x.NoError(s.Close())
			x.NoError(g.Close())
			x.Equal([]string{"open k", "stop k"}, r.Log())

			time.Sleep(2 * time.Second)
			synctest.Wait()
			x.Equal([]string{"open k", "stop k"}, r.Log(), "stopped once")
		})
	})
	t.Run("an upstream that ended does not linger", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source, Linger: time.Hour}

		s, err := g.Subscribe("k")
		x.NoError(err)
		r.emitter("k").End(nil)
		x.NoError(s.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
}

func TestEnd(t *testing.T) {
	t.Run("ending closes every subscriber with the error", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		a, err := g.Subscribe("k", streamflight.WithBuffer(4))
		x.NoError(err)
		var b collector
		sb, err := g.SubscribeFunc("k", b.Add)
		x.NoError(err)

		e := r.emitter("k")
		e.Emit(1)
		e.End(boom)
		x.Equal(0, e.Emit(2), "emit after end does nothing")
		e.End(errors.New("ignored"))

		<-a.Done()
		x.ErrorIs(a.Err(), boom)
		x.ErrorIs(sb.Err(), boom)
		x.Equal([]int{1}, drain(a.C))
		x.True(closed(a.C))
		x.Equal([]int{1}, b.Values())

		x.NoError(a.Close())
		x.Equal([]string{"open k"}, r.Log(), "stopped once every subscriber has closed")
		x.NoError(sb.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("a nil error ends with io.EOF", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k")
		x.NoError(err)
		r.emitter("k").End(nil)
		x.ErrorIs(s.Err(), io.EOF)
		x.NoError(s.Close())
	})
	t.Run("the next subscriber opens a fresh upstream after stopping the old one", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		old, err := g.Subscribe("k")
		x.NoError(err)
		r.emitter("k").End(nil)

		s, err := g.Subscribe("k")
		x.NoError(err)
		x.Equal([]string{"open k", "stop k", "open k"}, r.Log())
		x.NoError(s.Err())

		x.NoError(old.Close(), "the old upstream is already stopped")
		x.Equal([]string{"open k", "stop k", "open k"}, r.Log())
		x.NoError(s.Close())
		x.Equal([]string{"open k", "stop k", "open k", "stop k"}, r.Log())
	})
	t.Run("an upstream that ends while opening ends its first subscriber", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		stops := 0
		g := &streamflight.Group[string, int]{
			Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
				e.End(boom)
				return func() error { stops++; return nil }, nil
			},
		}

		s, err := g.Subscribe("k")
		x.NoError(err)
		<-s.Done()
		x.ErrorIs(s.Err(), boom)
		x.NoError(s.Close())
		x.Equal(1, stops)
	})
	t.Run("an upstream that ends and then fails to open is not stopped", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		stopped := false
		g := &streamflight.Group[string, int]{
			Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
				e.End(nil)
				return func() error { stopped = true; return nil }, boom
			},
		}

		_, err := g.Subscribe("k")
		x.ErrorIs(err, boom)
		x.False(stopped)
	})
}

func TestGroupClose(t *testing.T) {
	t.Run("closing the group stops every upstream and ends every subscription", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		r := newRecorder()
		r.stopErr = boom
		g := &streamflight.Group[string, int]{Source: r.Source}

		a, err := g.Subscribe("a")
		x.NoError(err)
		b, err := g.Subscribe("b")
		x.NoError(err)

		err = g.Close()
		x.ErrorIs(err, boom)
		x.ElementsMatch([]string{"open a", "open b", "stop a", "stop b"}, r.Log())
		x.ErrorIs(a.Err(), streamflight.ErrGroupClosed)
		x.ErrorIs(b.Err(), streamflight.ErrGroupClosed)
		x.True(closed(a.C))

		x.NoError(g.Close(), "idempotent")
		x.NoError(a.Close(), "already stopped")
		x.NoError(b.Close())
		x.Len(r.Log(), 4)

		_, err = g.Subscribe("a")
		x.ErrorIs(err, streamflight.ErrGroupClosed)
		_, err = g.SubscribeFunc("a", func(int) {})
		x.ErrorIs(err, streamflight.ErrGroupClosed)
	})
	t.Run("closing an empty group", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{Source: newRecorder().Source}
		x.NoError(g.Close())
	})
	t.Run("closing waits for a stop already in progress", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		src, entered, release := gatedStops(r.Source)
		g := &streamflight.Group[string, int]{Source: src}

		s, err := g.Subscribe("k")
		x.NoError(err)
		go s.Close()
		x.Equal("k", <-entered) // parked inside the stop func

		closed := make(chan error, 1)
		go func() { closed <- g.Close() }()
		time.Sleep(time.Millisecond) // let Close park on the stop in progress
		select {
		case <-closed:
			x.Fail("Close returned while an upstream was still stopping")
		default:
		}

		release()
		x.NoError(<-closed)
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("a second close waits for the first", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		src, entered, release := gatedStops(r.Source)
		g := &streamflight.Group[string, int]{Source: src}

		s, err := g.Subscribe("k")
		x.NoError(err)
		defer s.Close()

		first := make(chan error, 1)
		go func() { first <- g.Close() }()
		x.Equal("k", <-entered) // the first Close is inside the stop func

		second := make(chan error, 1)
		go func() { second <- g.Close() }()
		time.Sleep(time.Millisecond)
		select {
		case <-second:
			x.Fail("the second Close returned before every upstream was stopped")
		default:
		}

		release()
		x.NoError(<-first)
		x.NoError(<-second)
	})
}

// TestGroupIsolation pins that a delivery that stalls stalls only its own key.
// A Block subscriber holds the key's lock for as long as it is full, so
// anything the Group does under its own lock must never wait for a key's lock.
func TestGroupIsolation(t *testing.T) {
	// stall opens "stalled" with a delivery parked on a Block subscriber that
	// nobody reads, and returns once the emitting goroutine holds the key's
	// lock. Then it wedges a second subscriber of that same key against it,
	// which is what used to take the Group's lock and wait for the key's.
	stall := func(t *testing.T, g *streamflight.Group[string, int], r *recorder) {
		t.Helper()

		// Delivered to before the Block subscriber, so it reports that the
		// emitting goroutine is inside the delivery loop, holding the lock.
		entered := make(chan struct{}, 8)
		_, err := g.SubscribeFunc("stalled", func(int) { entered <- struct{}{} })
		require.NoError(t, err)
		_, err = g.Subscribe("stalled", streamflight.WithOverflow(streamflight.Block))
		require.NoError(t, err)

		r.emit("stalled", 1) // fills the Block subscriber's queue
		<-entered
		go r.emit("stalled", 2) // parks on it, holding the key's lock
		<-entered

		go g.Subscribe("stalled")    // wedges against the parked delivery
		time.Sleep(time.Millisecond) // let it reach into the Group
	}

	// within reports rather than hanging the suite when the Group is frozen.
	// It must not Fatal: the cleanup that would follow needs the Group too.
	within := func(t *testing.T, what string, fn func()) {
		t.Helper()
		done := make(chan struct{})
		go func() { defer close(done); fn() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("%s did not return: one key's stalled delivery froze the whole Group", what)
		}
	}

	t.Run("a stalled delivery does not block another key", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}
		stall(t, g, r)

		within(t, "Subscribe on an unrelated key", func() {
			s, err := g.Subscribe("other")
			x.NoError(err)
			x.NoError(s.Close())
		})
		within(t, "Close", func() { x.NoError(g.Close()) })
	})
	t.Run("a stalled delivery does not block closing the group", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}
		stall(t, g, r)

		// Closing the Group is one of the three things documented to release a
		// Block delivery, so it must not be the thing the delivery blocks.
		within(t, "Close", func() { x.NoError(g.Close()) })
	})
}

// gatedStops wraps a Source so that the first stop of each key parks until the
// returned release is called, with entered reporting that it got there.
func gatedStops(inner streamflight.Source[string, int]) (src streamflight.Source[string, int], entered <-chan string, release func()) {
	in := make(chan string, 8)
	gate := make(chan struct{})
	var once sync.Once
	var gated sync.Map
	return func(key string, e streamflight.Emitter[int]) (func() error, error) {
		stop, err := inner(key, e)
		if err != nil {
			return nil, err
		}
		return func() error {
			if _, dup := gated.LoadOrStore(key, true); !dup {
				in <- key
				<-gate
			}
			if stop == nil {
				return nil
			}
			return stop()
		}, nil
	}, in, func() { once.Do(func() { close(gate) }) }
}

// TestConcurrentOpen pins that a key being opened is not a key being held by
// the Group: the Source runs with no Group lock held, so other keys carry on,
// and everyone waiting for this one shares its single attempt.
func TestConcurrentOpen(t *testing.T) {
	t.Run("opening one key does not block another", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			gate := make(chan struct{})
			g := &streamflight.Group[string, int]{
				Source: func(key string, _ streamflight.Emitter[int]) (func() error, error) {
					if key == "slow" {
						<-gate
					}
					return nil, nil
				},
			}

			go g.Subscribe("slow")
			synctest.Wait() // parked inside the Source of "slow"

			fast, err := g.Subscribe("fast")
			x.NoError(err, "a second key opened while the first was still opening")
			x.NoError(fast.Close())

			close(gate)
			synctest.Wait()
			x.NoError(g.Close())
		})
	})
	t.Run("concurrent subscribers share one failed open", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			boom := errors.New("boom")
			gate := make(chan struct{})
			opens := 0
			g := &streamflight.Group[string, int]{
				Source: func(string, streamflight.Emitter[int]) (func() error, error) {
					opens++
					<-gate
					return nil, boom
				},
			}

			const n = 8
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i := range errs {
				wg.Go(func() { _, errs[i] = g.Subscribe("k") })
			}
			synctest.Wait() // one is in the Source, the rest wait on it

			close(gate)
			wg.Wait()
			for _, err := range errs {
				x.ErrorIs(err, boom, "the waiters get the opener's error")
			}
			x.Equal(1, opens, "one attempt, shared by all of them")

			// The key was given back, so it can be opened again.
			x.NoError(g.Close())
		})
	})
	t.Run("closing during an open stops the upstream it opened", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			gate := make(chan struct{})
			g := &streamflight.Group[string, int]{
				Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
					<-gate
					return r.Source(key, e)
				},
			}

			sub := make(chan error, 1)
			go func() { _, err := g.Subscribe("k"); sub <- err }()
			synctest.Wait() // parked inside the Source

			closed := make(chan error, 1)
			go func() { closed <- g.Close() }()
			synctest.Wait() // Close is waiting for the open to finish

			close(gate)
			x.ErrorIs(<-sub, streamflight.ErrGroupClosed)
			x.NoError(<-closed)
			x.Equal([]string{"open k", "stop k"}, r.Log(), "what was opened was stopped")
		})
	})
}

// TestConcurrentStop pins that a key being stopped is not a key being held by
// the Group: the stop func runs with no Group lock held, so everything else
// carries on, and whoever wants the key next waits for the stop rather than
// racing it.
func TestConcurrentStop(t *testing.T) {
	t.Run("a Source may use the Group while it is being stopped", func(t *testing.T) {
		// The fan-in shape a gateway reaches for: one key is built out of
		// another, and releases it on the way out. Its stop waits for that
		// goroutine, so the Group must not be locked while it runs.
		x := require.New(t)
		g := &streamflight.Group[string, int]{}
		g.Source = func(key string, e streamflight.Emitter[int]) (func() error, error) {
			if key == "raw" {
				return func() error { return nil }, nil
			}
			return streamflight.Run(func(ctx context.Context, _ string, e streamflight.Emitter[int]) error {
				inner, err := g.SubscribeFunc("raw", func(v int) { e.Emit(v) })
				if err != nil {
					return err
				}
				defer inner.Close() // needs the Group, while stop waits for us
				<-ctx.Done()
				return nil
			})(key, e)
		}

		s, err := g.SubscribeFunc("derived", func(int) {})
		x.NoError(err)
		time.Sleep(10 * time.Millisecond) // let the inner subscription establish

		done := make(chan error, 1)
		go func() { done <- s.Close() }()
		select {
		case err := <-done:
			x.NoError(err)
		case <-time.After(5 * time.Second):
			x.Fail("Close hung: stopping one key blocked the Group a Source needed")
		}
		x.NoError(g.Close())
	})
	t.Run("a subscriber waits for the stop in progress and opens a fresh upstream", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		src, entered, release := gatedStops(r.Source)
		g := &streamflight.Group[string, int]{Source: src}

		s, err := g.Subscribe("k")
		x.NoError(err)
		go s.Close()
		x.Equal("k", <-entered) // parked inside the stop func

		joined := make(chan *streamflight.Subscription[int], 1)
		go func() {
			s2, err := g.Subscribe("k")
			x.NoError(err)
			joined <- s2
		}()
		time.Sleep(time.Millisecond) // let it park on the stop in progress
		x.Equal([]string{"open k"}, r.Log(), "the fresh upstream waits for the stop")

		release()
		s2 := <-joined
		x.Equal([]string{"open k", "stop k", "open k"}, r.Log())
		x.NoError(s2.Close())
		x.Equal([]string{"open k", "stop k", "open k", "stop k"}, r.Log())
	})
	t.Run("concurrent subscribers of an ended upstream stop it once", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		old, err := g.Subscribe("k")
		x.NoError(err)
		r.emitter("k").End(nil)

		const n = 16
		subs := make([]*streamflight.Subscription[int], n)
		var wg sync.WaitGroup
		for i := range subs {
			wg.Go(func() {
				s, err := g.Subscribe("k")
				x.NoError(err)
				subs[i] = s
			})
		}
		wg.Wait()

		x.Equal([]string{"open k", "stop k", "open k"}, r.Log(), "stopped once, re-opened once")
		x.NoError(old.Close())
		for _, s := range subs {
			x.NoError(s.Close())
		}
		x.Equal([]string{"open k", "stop k", "open k", "stop k"}, r.Log())
	})
}

func TestHooks(t *testing.T) {
	x := require.New(t)
	boom := errors.New("boom")
	r := newRecorder()

	var log []string
	g := &streamflight.Group[string, int]{
		Source: r.Source,
		Hooks: streamflight.Hooks[string, int]{
			Opened:  func(key string, err error) { log = append(log, fmt.Sprintf("opened %s %v", key, err)) },
			Stopped: func(key string, err error) { log = append(log, fmt.Sprintf("stopped %s %v", key, err)) },
			Joined:  func(key string, n int) { log = append(log, fmt.Sprintf("joined %s %d", key, n)) },
			Left:    func(key string, n int) { log = append(log, fmt.Sprintf("left %s %d", key, n)) },
			Dropped: func(key string, v int) { log = append(log, fmt.Sprintf("dropped %s %d", key, v)) },
		},
	}

	a, err := g.Subscribe("k")
	x.NoError(err)
	b, err := g.Subscribe("k")
	x.NoError(err)
	r.emit("k", 1, 2)
	x.NoError(a.Close())
	x.NoError(b.Close())

	r.set(func(r *recorder) { r.openErr = boom })
	_, err = g.Subscribe("k")
	x.ErrorIs(err, boom)

	x.Equal([]string{
		"opened k <nil>",
		"joined k 1",
		"joined k 2",
		"dropped k 1",
		"dropped k 1",
		"left k 1",
		"left k 0",
		"stopped k <nil>",
		"opened k boom",
	}, log)
}

func TestDelivery(t *testing.T) {
	t.Run("a subscriber is never delivered to concurrently", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		var inflight atomic.Int32
		var overlapped atomic.Bool
		total := 0 // unsynchronized on purpose: the race detector flags concurrent deliveries
		s, err := g.SubscribeFunc("k", func(int) {
			if inflight.Add(1) > 1 {
				overlapped.Store(true)
			}
			total++
			inflight.Add(-1)
		})
		x.NoError(err)

		const emitters, each = 8, 1000
		e := r.emitter("k")
		var wg sync.WaitGroup
		for range emitters {
			wg.Go(func() {
				for range each {
					e.Emit(1)
				}
			})
		}
		wg.Wait()

		x.False(overlapped.Load())
		x.Equal(emitters*each, total)
		x.NoError(s.Close())
	})
	t.Run("nothing is delivered after close returns", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		var kept atomic.Int64
		keep, err := g.SubscribeFunc("k", func(int) { kept.Add(1) })
		x.NoError(err)

		var closedAt, late atomic.Bool
		s, err := g.SubscribeFunc("k", func(int) {
			if closedAt.Load() {
				late.Store(true)
			}
		})
		x.NoError(err)

		e := r.emitter("k")
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				default:
					e.Emit(1)
				}
			}
		}()

		for kept.Load() < 100 {
			time.Sleep(time.Microsecond)
		}
		x.NoError(s.Close())
		closedAt.Store(true)

		target := kept.Load() + 1000
		for kept.Load() < target {
			time.Sleep(time.Microsecond)
		}
		close(stop)
		<-done

		x.False(late.Load())
		x.NoError(keep.Close())
	})
	t.Run("values arrive in the order they were emitted", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k", streamflight.WithBuffer(100), streamflight.WithOverflow(streamflight.Block))
		x.NoError(err)
		want := make([]int, 100)
		for i := range want {
			want[i] = i
			r.emit("k", i)
		}
		x.Equal(want, drain(s.C))
		x.NoError(s.Close())
	})
}

// upstreams tracks what the Group did to every key, so a soak can assert the
// lifecycle guarantees rather than just that nothing crashed.
type upstreams struct {
	t *testing.T

	mu    sync.Mutex
	live  map[string]bool
	opens int
	stops int
}

func newUpstreams(t *testing.T) *upstreams {
	return &upstreams{t: t, live: map[string]bool{}}
}

// Source emits an increasing counter until it is stopped, and ends by itself
// every so often so that the take-over-and-re-open path is exercised too.
func (u *upstreams) Source(key string, e streamflight.Emitter[int]) (func() error, error) {
	u.mu.Lock()
	if u.live[key] {
		u.t.Errorf("two upstreams open at once for %q", key)
	}
	u.live[key] = true
	u.opens++
	endAfter := 0
	if u.opens%4 == 0 {
		endAfter = 5 // this one ends by itself
	}
	u.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if endAfter > 0 && i > endAfter {
				e.End(io.EOF)
				return
			}
			e.Emit(i)
			runtime.Gosched()
		}
	}()

	return func() error {
		cancel()
		<-done // no emitting goroutine outlives its upstream

		u.mu.Lock()
		defer u.mu.Unlock()
		if !u.live[key] {
			u.t.Errorf("upstream of %q stopped twice", key)
		}
		u.live[key] = false
		u.stops++
		return nil
	}, nil
}

// snapshot is what Initial sends in the soak below. Negative so that it cannot
// be mistaken for one of the counter values an upstream emits.
const snapshot = -1

// TestMultipleClients is a soak: many clients joining and leaving a small pool
// of keys at once, in every subscription style, while their upstreams emit,
// end by themselves and are re-opened underneath them.
func TestMultipleClients(t *testing.T) {
	// Without Linger every key that goes idle is stopped and re-opened, which
	// is the churn the lifecycle guarantees are about. With it, the same clients
	// mostly rejoin an upstream that is still running.
	t.Run("without linger", func(t *testing.T) { soak(t, 0) })
	t.Run("with linger", func(t *testing.T) { soak(t, time.Millisecond) })
}

func soak(t *testing.T, linger time.Duration) {
	const (
		keys    = 6
		clients = 48
		rounds  = 60
	)
	x := require.New(t)
	u := newUpstreams(t)
	g := &streamflight.Group[string, int]{
		Source: u.Source,
		Replay: 2,
		Linger: linger,
		// A snapshot, not an emitted value: it is sent per subscriber after the
		// replay, so it is deliberately out of band and excluded below.
		Initial: func(_ string, send func(int)) { send(snapshot) },
	}

	// increasing checks the ordering guarantee: a subscriber may lose values to
	// its Overflow policy, but never sees the ones it gets go backwards. It
	// applies to emitted values only, which is what the guarantee covers.
	increasing := func(what string, prev *int, v int) {
		if v == snapshot {
			return
		}
		if v < *prev {
			t.Errorf("%s: out of order, %d after %d", what, v, *prev)
		}
		*prev = v
	}

	var wg sync.WaitGroup
	for c := range clients {
		wg.Go(func() {
			for round := range rounds {
				key := fmt.Sprintf("k%d", (c+round)%keys)

				switch (c + round) % 4 {
				case 0: // a function subscriber
					var prev int
					var after atomic.Bool
					var late atomic.Bool
					s, err := g.SubscribeFunc(key, func(v int) {
						if after.Load() {
							late.Store(true)
						}
						increasing("SubscribeFunc", &prev, v)
					})
					if err != nil {
						return
					}
					runtime.Gosched()
					x.NoError(s.Close())
					after.Store(true)
					runtime.Gosched()
					x.False(late.Load(), "delivered after Close returned")

				default: // a channel subscriber, under each policy
					// Block included on purpose: a subscriber that stops reading
					// holds its key's delivery, so this is where a Group that
					// couples keys together seizes up.
					policy := []streamflight.Overflow{
						streamflight.DropOldest, streamflight.DropNewest,
						streamflight.Evict, streamflight.Block,
					}[(c+round)%4]
					s, err := g.Subscribe(key,
						streamflight.WithBuffer(1+(c+round)%8),
						streamflight.WithOverflow(policy))
					if err != nil {
						return
					}
					prev := 0
					for range 3 {
						select {
						case v, ok := <-s.C:
							if ok {
								increasing("Subscribe", &prev, v)
							}
						default:
						}
						runtime.Gosched()
					}
					x.NoError(s.Close())
					for v := range s.C { // what was queued is still readable
						increasing("Subscribe after close", &prev, v)
					}
				}
			}
		})
	}
	wg.Wait()

	x.NoError(g.Close())

	u.mu.Lock()
	defer u.mu.Unlock()
	x.Equal(u.opens, u.stops, "every upstream opened was stopped exactly once")
	for key, live := range u.live {
		x.False(live, "upstream of %q still running after Close", key)
	}
	t.Logf("%d clients x %d rounds over %d keys: %d upstreams opened and stopped",
		clients, rounds, keys, u.opens)
}

func TestPoll(t *testing.T) {
	t.Run("the first tick is on open and the next an interval after the last returned", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			// The first tick outruns the interval. A Ticker would have kept the
			// tick it missed and fired again at once, leaving no idle at all.
			var at []time.Duration
			start := time.Now()
			g := &streamflight.Group[string, int]{
				Replay: 4,
				Source: streamflight.Poll(time.Second,
					func(_ context.Context, _ string, e streamflight.Emitter[int]) error {
						at = append(at, time.Since(start))
						if len(at) == 1 {
							time.Sleep(3 * time.Second)
						}
						e.Emit(len(at))
						return nil
					}),
			}

			s, err := g.Subscribe("k", streamflight.WithBuffer(8))
			x.NoError(err)
			time.Sleep(5500 * time.Millisecond)
			synctest.Wait()

			x.Equal([]time.Duration{0, 4 * time.Second, 5 * time.Second}, at,
				"quiet time between calls, not a deadline the work can miss")
			x.NoError(s.Close())
			x.NoError(g.Close())
		})
	})
	t.Run("the opener receives the first tick through Replay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			g := &streamflight.Group[string, int]{
				Replay: 1,
				Source: streamflight.Poll(time.Hour,
					func(_ context.Context, _ string, e streamflight.Emitter[int]) error {
						e.Emit(7)
						return nil
					}),
			}

			s, err := g.Subscribe("k")
			x.NoError(err)
			x.Equal(7, <-s.C, "on open, not after one interval")
			x.NoError(s.Close())
		})
	})
	t.Run("the poll stops with its key", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			ticks := 0
			g := &streamflight.Group[string, int]{
				Replay: 1,
				Source: streamflight.Poll(time.Second,
					func(_ context.Context, _ string, e streamflight.Emitter[int]) error {
						ticks++
						e.Emit(ticks)
						return nil
					}),
			}

			s, err := g.Subscribe("k", streamflight.WithBuffer(8))
			x.NoError(err)
			time.Sleep(2500 * time.Millisecond)
			synctest.Wait()
			x.NoError(s.Close())

			was := ticks
			time.Sleep(5 * time.Second)
			synctest.Wait()
			x.Equal(was, ticks, "nothing polls a key nobody holds")
		})
	})
	t.Run("a tick that fails ends the upstream with its error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			boom := errors.New("boom")
			g := &streamflight.Group[string, int]{
				Source: streamflight.Poll(time.Second,
					func(context.Context, string, streamflight.Emitter[int]) error { return boom }),
			}

			s, err := g.Subscribe("k")
			x.NoError(err)
			synctest.Wait()
			<-s.Done()
			x.ErrorIs(s.Err(), boom)
			x.NoError(s.Close())
		})
	})
	t.Run("an interval that is not positive fails the subscribe", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{
			Source: streamflight.Poll(0,
				func(context.Context, string, streamflight.Emitter[int]) error { return nil }),
		}

		s, err := g.Subscribe("k")
		x.ErrorIs(err, streamflight.ErrPollInterval, "spinning is not a poll")
		x.Nil(s)
	})
}

func TestRun(t *testing.T) {
	t.Run("a poller runs while subscribed and is stopped with its context", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			returned := make(chan error, 1)
			g := &streamflight.Group[string, int]{
				Source: streamflight.Run(func(ctx context.Context, key string, e streamflight.Emitter[int]) error {
					tick := time.NewTicker(time.Second)
					defer tick.Stop()
					for i := 1; ; i++ {
						select {
						case <-ctx.Done():
							returned <- ctx.Err()
							return ctx.Err()
						case <-tick.C:
							e.Emit(i)
						}
					}
				}),
			}

			s, err := g.Subscribe("k", streamflight.WithBuffer(8))
			x.NoError(err)
			time.Sleep(3 * time.Second)
			synctest.Wait()
			x.Equal([]int{1, 2, 3}, drain(s.C))

			x.NoError(s.Close())
			x.ErrorIs(<-returned, context.Canceled)
			x.ErrorIs(s.Err(), streamflight.ErrClosed, "stopping is not an end")
		})
	})
	t.Run("a function that returns ends the upstream with its error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			boom := errors.New("boom")
			g := &streamflight.Group[string, int]{
				Source: streamflight.Run(func(ctx context.Context, key string, e streamflight.Emitter[int]) error {
					e.Emit(1)
					return boom
				}),
			}

			s, err := g.Subscribe("k", streamflight.WithBuffer(8))
			x.NoError(err)
			synctest.Wait()
			<-s.Done()
			x.ErrorIs(s.Err(), boom)
			x.NoError(s.Close())
		})
	})
}
