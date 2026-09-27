package streamflight_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"weak"

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

// returns runs f and fails the test if f has not returned within a few
// seconds, which is what a wedged Group looks like from outside. It fails
// rather than hangs, so a regression names itself. Not in a synctest bubble,
// though: there a wait on a mutex does not let the clock move, so only a wait
// on a channel turns into a failure, and a wedge on a lock hangs until the
// test binary times out.
func returns(t *testing.T, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("did not return: the Group is wedged")
	}
}

// helping reports whether a goroutine is waiting for a key's lock on behalf of
// a Context subscribe that found it held.
func helping() bool { return running(").help(") > 0 }

// running counts the goroutines that are in a call to fn, named as a stack
// trace shows it, such as ").expire(" for a Linger timer that has fired.
func running(fn string) int {
	buf := make([]byte, 1<<20)
	return strings.Count(string(buf[:runtime.Stack(buf, true)]), fn)
}

// eventually fails the test unless cond comes true within a few seconds.
func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("never: " + what)
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
	t.Run("a closed subscription does not keep what its stopped upstream held", func(t *testing.T) {
		x := require.New(t)
		var held weak.Pointer[[1 << 16]byte]
		g := &streamflight.Group[string, int]{
			Source: func(string, streamflight.Emitter[int]) (func() error, error) {
				conn := new([1 << 16]byte) // what a stop closes: a connection, a buffer
				held = weak.Make(conn)
				return func() error { conn[0] = 1; return nil }, nil
			},
		}

		s, err := g.Subscribe("k")
		x.NoError(err)
		x.NoError(s.Close())
		runtime.GC()
		x.Nil(held.Value(), "its stop has been called and is not called again")
		runtime.KeepAlive(s) // held, as a caller that reads Err or Latest later does
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
	t.Run("once the context is done it sends nothing more, and loses nothing", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k", streamflight.WithBuffer(8))
		x.NoError(err)
		defer s.Close()
		r.emit("k", 1, 2)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for range 100 { // select alone would pick the queue about half the time
			x.NoError(s.Drain(ctx, func(int) error {
				x.Fail("sent to a client that is gone")
				return nil
			}))
		}

		again, stop := context.WithCancel(context.Background())
		var got []int
		x.NoError(s.Drain(again, func(v int) error {
			got = append(got, v)
			if len(got) == 2 {
				stop()
			}
			return nil
		}))
		x.Equal([]int{1, 2}, got, "what was queued waits for the next Drain")
	})
	t.Run("a context that ends while it waits ends it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			g := &streamflight.Group[string, int]{Source: newRecorder().Source}
			s, err := g.Subscribe("k")
			x.NoError(err)
			defer s.Close()

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- s.Drain(ctx, func(int) error { return nil }) }()
			synctest.Wait() // waiting on an empty queue
			cancel()
			x.NoError(<-done)
		})
	})
	t.Run("a send that fails because the client went away is not a failure", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		s, err := g.Subscribe("k")
		x.NoError(err)
		defer s.Close()
		r.emit("k", 1)

		ctx, cancel := context.WithCancel(context.Background())
		x.NoError(s.Drain(ctx, func(int) error {
			cancel()
			return context.Canceled // as a stream's write does once its client has gone
		}))
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
	t.Run("Replay and Initial give a sampler nothing, and each value is counted for it", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{
			Source:  r.Source,
			Replay:  2,
			Initial: func(_ string, send func(int)) { send(9) },
		}

		var keep collector
		k, err := g.SubscribeFunc("k", keep.Add)
		x.NoError(err)
		r.emit("k", 1, 2)

		s, err := g.SubscribeLatest("k")
		x.NoError(err)
		_, _, ok := s.Latest()
		x.False(ok, "a key someone else opened has nothing to sample until its next value")

		x.Equal(2, r.emit("k", 3), "a sampler takes every value")
		v, _, ok := s.Latest()
		x.True(ok)
		x.Equal(3, v)

		x.NoError(s.Close())
		x.Equal(1, r.emit("k", 4), "one that has left takes none")
		x.Equal([]int{9, 1, 2, 3, 4}, keep.Values())
		x.NoError(k.Close())
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
	t.Run("waiting past the zero time finds a value whatever the clock says", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Now:    func() time.Time { return time.Time{} }, // a fake clock at its zero
		}

		s, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer s.Close()
		r.emit("k", 7)

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		v, _, ok := s.Wait(ctx, time.Time{})
		x.True(ok, "the value there is after the zero time")
		x.Equal(7, v)
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
		fn, err := g.SubscribeFunc("k", func(int) {})
		x.NoError(err)
		defer fn.Close()

		const msg = "streamflight: Wait on a subscription that is delivered to"
		x.PanicsWithValue(msg, func() { ch.Wait(context.Background(), time.Time{}) })
		x.PanicsWithValue(msg, func() { fn.Wait(context.Background(), time.Time{}) })
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
	t.Run("one Emit that evicts several keeps the rest, in order", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		var got []string
		sub := func(name string, opts ...streamflight.SubscribeOption) *streamflight.Subscription[int] {
			if opts == nil {
				s, err := g.SubscribeFunc("k", func(v int) { got = append(got, fmt.Sprint(name, v)) })
				x.NoError(err)
				return s
			}
			s, err := g.Subscribe("k", opts...)
			x.NoError(err)
			return s
		}
		evict := streamflight.WithOverflow(streamflight.Evict)
		a := sub("a", evict)
		f1 := sub("f1")
		b := sub("b", evict)
		refuse := sub("refuse", streamflight.WithOverflow(streamflight.DropNewest))
		c := sub("c", evict)
		f2 := sub("f2")

		x.Equal(6, r.emit("k", 1), "every queue takes one")
		x.Equal(2, r.emit("k", 2), "a, b and c are evicted, refuse refuses, f1 and f2 take it")
		for _, s := range []*streamflight.Subscription[int]{a, b, c} {
			x.ErrorIs(s.Err(), streamflight.ErrEvicted)
		}
		x.Equal(2, r.emit("k", 3), "refuse is still full; the evicted are gone")
		x.Equal([]string{"f11", "f21", "f12", "f22", "f13", "f23"}, got, "the ones that stayed, in their order")
		x.Equal([]int{1}, drain(refuse.C))
		x.Equal(uint64(2), refuse.Dropped())

		for _, s := range []*streamflight.Subscription[int]{a, f1, b, refuse, c, f2} {
			x.NoError(s.Close())
		}
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("a delivery that panics after an eviction leaves the subscribers whole", func(t *testing.T) {
		const boom = "boom"
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		evict := streamflight.WithOverflow(streamflight.Evict)
		a, err := g.Subscribe("k", evict)
		x.NoError(err)
		var fn collector
		f, err := g.SubscribeFunc("k", func(v int) {
			if v == 2 {
				panic(boom)
			}
			fn.Add(v)
		})
		x.NoError(err)
		b, err := g.Subscribe("k", evict)
		x.NoError(err)
		keep, err := g.Subscribe("k", streamflight.WithBuffer(8))
		x.NoError(err)

		r.emit("k", 1)
		x.PanicsWithValue(boom, func() { r.emit("k", 2) }, "a was evicted, then the function panicked")
		x.ErrorIs(a.Err(), streamflight.ErrEvicted)
		x.NoError(b.Err(), "not reached, so kept")

		x.Equal(2, r.emit("k", 3), "no send to a's closed queue; b is evicted now")
		x.ErrorIs(b.Err(), streamflight.ErrEvicted)
		x.Equal([]int{1, 3}, fn.Values())
		x.Equal([]int{1, 3}, drain(keep.C))

		for _, s := range []*streamflight.Subscription[int]{a, f, b, keep} {
			x.NoError(s.Close())
		}
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("Evict ends a subscriber whose catch-up does not fit its queue", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		initials := 0
		g := &streamflight.Group[string, int]{
			Source:  r.Source,
			Replay:  3,
			Initial: func(string, func(int)) { initials++ },
		}

		var keep collector
		k, err := g.SubscribeFunc("k", keep.Add)
		x.NoError(err)
		r.emit("k", 1, 2, 3)
		x.Equal(1, initials)

		s, err := g.Subscribe("k", streamflight.WithBuffer(2), streamflight.WithOverflow(streamflight.Evict))
		x.NoError(err, "returned already ended, like a subscription to an upstream that has ended")
		x.ErrorIs(s.Err(), streamflight.ErrEvicted, "a gap in the catch-up is a gap like any other")
		x.Equal([]int{1, 2}, drain(s.C), "what fitted before the gap")
		x.Zero(s.Dropped(), "cut off, not dropped from")
		x.Equal(1, initials, "nothing more is sent to a subscriber that is gone")

		x.Equal(1, r.emit("k", 4), "and nothing live")
		x.Equal([]int{1, 2, 3, 4}, keep.Values(), "nobody else is affected")
		x.NoError(s.Close())
		x.Equal([]string{"open k"}, r.Log(), "Close gave back its reference, and k still holds the key")
		x.NoError(k.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("Evict ends a subscriber whose Initial does not fit its queue", func(t *testing.T) {
		x := require.New(t)
		sends := 0
		g := &streamflight.Group[string, int]{
			Source: newRecorder().Source,
			Initial: func(_ string, send func(int)) {
				for v := 1; v <= 3; v++ {
					send(v)
					sends++
				}
			},
		}

		whole, err := g.Subscribe("k", streamflight.WithBuffer(3), streamflight.WithOverflow(streamflight.Evict))
		x.NoError(err)
		x.NoError(whole.Err(), "a snapshot that fits is received whole")
		x.Equal([]int{1, 2, 3}, drain(whole.C))

		cut, err := g.Subscribe("k", streamflight.WithBuffer(2), streamflight.WithOverflow(streamflight.Evict))
		x.NoError(err)
		x.ErrorIs(cut.Err(), streamflight.ErrEvicted, "a truncated snapshot is a corrupt base")
		x.Equal([]int{1, 2}, drain(cut.C))
		x.Equal(6, sends, "a send after the cut does nothing, and does not panic")

		x.NoError(cut.Close())
		x.NoError(whole.Close())
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
	t.Run("a short queue keeps the newest replayed values under DropNewest too", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var dropped []int
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Replay: 3,
			Hooks: streamflight.Hooks[string, int]{
				Dropped: func(_ string, v int) { dropped = append(dropped, v) },
			},
		}

		keep, err := g.SubscribeFunc("k", func(int) {})
		x.NoError(err)
		r.emit("k", 1, 2, 3)

		s, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.DropNewest))
		x.NoError(err)
		x.Equal([]int{3}, drain(s.C), "catching up keeps the newest, whatever the policy")
		x.Equal(uint64(2), s.Dropped())
		x.Equal([]int{1, 2}, dropped, "the displaced values, not the arriving ones")

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
	t.Run("a short queue keeps the newest of what Initial sends, under any policy but Evict", func(t *testing.T) {
		for _, o := range []streamflight.Overflow{streamflight.DropOldest, streamflight.DropNewest, streamflight.Block} {
			t.Run(fmt.Sprint(o), func(t *testing.T) {
				x := require.New(t)
				g := &streamflight.Group[string, int]{
					Source: newRecorder().Source,
					Initial: func(_ string, send func(int)) {
						send(1)
						send(2)
						send(3)
					},
				}

				var s *streamflight.Subscription[int]
				var err error
				returns(t, func() { s, err = g.Subscribe("k", streamflight.WithOverflow(o)) })
				x.NoError(err, "never waited for, not even under Block")
				x.Equal([]int{3}, drain(s.C))
				x.Equal(uint64(2), s.Dropped())
				x.NoError(s.Close())
			})
		}
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
	t.Run("a nil function panics rather than sample instead", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		x.PanicsWithValue("streamflight: SubscribeFunc with a nil function", func() {
			g.SubscribeFunc("k", nil)
		})
		x.Empty(r.Log(), "before anything was opened")
	})
	t.Run("an unknown Overflow panics rather than drop some other way", func(t *testing.T) {
		x := require.New(t)
		const want = "; want DropOldest, DropNewest, Block or Evict"
		x.PanicsWithValue("streamflight: WithOverflow with an unknown Overflow 4"+want, func() {
			streamflight.WithOverflow(streamflight.Evict + 1)
		})
		x.PanicsWithValue("streamflight: WithOverflow with an unknown Overflow -1"+want, func() {
			streamflight.WithOverflow(-1)
		})
	})
	t.Run("a nil function panics under every name, before anything is opened", func(t *testing.T) {
		x := require.New(t)
		var opened atomic.Int64
		g := &streamflight.Group[string, int]{
			Source: func(string, streamflight.Emitter[int]) (func() error, error) {
				opened.Add(1)
				return nil, nil
			},
		}
		x.PanicsWithValue("streamflight: SubscribeFuncContext with a nil function", func() {
			g.SubscribeFuncContext(context.Background(), "k", nil)
		})
		x.PanicsWithValue("streamflight: Run with a nil function", func() {
			streamflight.Run[string, int](nil)
		})
		x.PanicsWithValue("streamflight: Poll with a nil function", func() {
			streamflight.Poll[string, int](time.Second, nil)
		})
		x.PanicsWithValue("streamflight: nil SubscribeOption", func() {
			var opt streamflight.SubscribeOption // left unset on some branch
			g.Subscribe("k", opt)
		})
		x.Zero(opened.Load())
	})
	t.Run("Drain and Wait refuse a nil context or send, and lose nothing doing it", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}
		var nilCtx context.Context

		s, err := g.Subscribe("k", streamflight.WithBuffer(4))
		x.NoError(err)
		defer s.Close()
		r.emit("k", 1)
		x.PanicsWithValue("streamflight: Drain with a nil send", func() {
			s.Drain(context.Background(), nil)
		})
		x.PanicsWithValue("streamflight: nil Context", func() {
			s.Drain(nilCtx, func(int) error { return nil })
		})
		x.Equal([]int{1}, drain(s.C), "the value is still there")

		l, err := g.SubscribeLatest("k")
		x.NoError(err)
		defer l.Close()
		r.emit("k", 2)
		x.PanicsWithValue("streamflight: nil Context", func() {
			l.Wait(nilCtx, time.Time{}) // even with a value there to return
		})
	})
	t.Run("a Subscription no Group made is refused", func(t *testing.T) {
		x := require.New(t)
		var zero streamflight.Subscription[int]
		// Done, so that Drain returns at once rather than hang if it ever
		// gets past the check.
		gone, cancel := context.WithCancel(context.Background())
		cancel()
		x.PanicsWithValue("streamflight: Drain on a subscription with no channel", func() {
			zero.Drain(gone, func(int) error { return nil })
		}, "rather than wait for a value that cannot come")
		x.PanicsWithValue("streamflight: Latest on a subscription that is delivered to", func() {
			zero.Latest()
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
	// Not in a synctest bubble, where waiting for the Group's lock does not let
	// the clock move: these need a timer that fires while the lock is held.
	t.Run("a subscriber back as the timer fires keeps the upstream", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		joins := 0
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Linger: 20 * time.Millisecond,
			Hooks: streamflight.Hooks[string, int]{
				Joined: func(string, int) {
					if joins++; joins == 2 {
						// The Group's lock is held: let the timer fire and
						// reach it before this subscriber has joined.
						eventually(t, func() bool { return running(").expire(") > 0 }, "the timer fires")
					}
				},
			},
		}

		a, err := g.Subscribe("k")
		x.NoError(err)
		x.NoError(a.Close())
		b, err := g.Subscribe("k")
		x.NoError(err)
		eventually(t, func() bool { return running(").expire(") == 0 }, "the timer gives up")

		x.Equal([]string{"open k"}, r.Log(), "the timer was for a subscriber who left, not for b")
		x.NoError(b.Err())
		x.Equal(1, r.emit("k", 1))
		x.NoError(b.Close())
	})
	t.Run("a timer that fires as the key is taken over does not stop it again", func(t *testing.T) {
		x := require.New(t)
		// With one P the timer reaches the lock only once the takeover is
		// over, and its guard has nothing left to guard against. Even with two
		// it seldom reaches it while the takeover is stopping the key, which
		// TestTimerFiringAsTheKeyIsTakenOver pins by hand.
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(max(2, runtime.GOMAXPROCS(0))))
		for range 20 { // it reaches the lock as the takeover stops the key, or after
			r := newRecorder()
			var g *streamflight.Group[string, int]
			var next *streamflight.Subscription[int]
			var took sync.WaitGroup
			g = &streamflight.Group[string, int]{
				Source: r.Source,
				Linger: 20 * time.Millisecond,
				Hooks: streamflight.Hooks[string, int]{
					Joined: func(key string, _ int) {
						if key != "hold" {
							return
						}
						// The Group's lock is held. Queue a subscriber that
						// takes over the ended key, and let the timer fire.
						took.Go(func() { next = must(g.Subscribe("k")) })
						eventually(t, func() bool { return running(").expire(") > 0 }, "the timer fires")
						time.Sleep(time.Millisecond) // for the subscriber to reach the lock
					},
				},
			}

			a, err := g.Subscribe("k")
			x.NoError(err)
			x.NoError(a.Close())
			r.emitter("k").End(nil) // lingering, and ended: the next subscriber takes it over
			hold, err := g.Subscribe("hold")
			x.NoError(err)
			took.Wait()

			x.NoError(next.Close())
			x.NoError(hold.Close())
			x.NoError(g.Close()) // rather than wait out the new upstream's Linger
			x.Equal(2, strings.Count(strings.Join(r.Log(), ","), "stop k"), "once for each upstream: %v", r.Log())
			// So that the next pass waits for its own timer, not this one.
			eventually(t, func() bool { return running(").expire(") == 0 }, "the timer returns")
		}
	})
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
		opens, stops := 0, 0
		g := &streamflight.Group[string, int]{
			Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
				if opens++; opens == 1 {
					e.End(boom)
				}
				return func() error { stops++; return nil }, nil
			},
		}

		s, err := g.Subscribe("k")
		x.NoError(err)
		<-s.Done()
		x.ErrorIs(s.Err(), boom)

		// Nobody waited on that open, so the next subscriber, s still held,
		// opens a fresh upstream rather than share the end.
		next, err := g.Subscribe("k")
		x.NoError(err)
		x.NoError(next.Err())
		x.Equal(2, opens)
		x.Equal(1, stops, "the ended upstream was stopped before the reopen")

		x.NoError(s.Close())
		x.NoError(next.Close())
		x.Equal(2, stops)
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

		x.Equal(err, g.Close(), "idempotent, and the same error every time")
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
		boom := errors.New("boom")
		r := newRecorder()
		r.stopErr = boom
		src, entered, release := gatedStops(r.Source)
		g := &streamflight.Group[string, int]{Source: src}

		s, err := g.Subscribe("k")
		x.NoError(err)
		left := make(chan error, 1)
		go func() { left <- s.Close() }()
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
		x.NoError(<-closed, "the stop was not Close's to report")
		x.ErrorIs(<-left, boom, "but the Subscription.Close's that ran it")
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("a second close waits for the first, and returns what it does", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		r := newRecorder()
		r.stopErr = boom
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
		x.ErrorIs(<-first, boom)
		x.ErrorIs(<-second, boom, "not nil because another Close ran the stops")
	})
	t.Run("a Block subscriber is ended, not refused values, while other keys stop", func(t *testing.T) {
		for range 10 { // a first or b first
			synctest.Test(t, func(t *testing.T) {
				x := require.New(t)
				gate := make(chan struct{})
				var b streamflight.Emitter[int]
				g := &streamflight.Group[string, int]{
					Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
						if key == "b" {
							b = e
							return nil, nil
						}
						return func() error { <-gate; return nil }, nil // a slow stop
					},
				}

				a, err := g.Subscribe("a")
				x.NoError(err)
				blk, err := g.Subscribe("b", streamflight.WithOverflow(streamflight.Block))
				x.NoError(err)
				b.Emit(1) // blk's queue is full

				closed := make(chan error, 1)
				go func() { closed <- g.Close() }()
				synctest.Wait() // whichever stop runs first, blk is ended before any

				x.ErrorIs(blk.Err(), streamflight.ErrGroupClosed, "not left live to be refused")
				x.Equal(0, b.Emit(2), "nobody left to take it")
				close(gate)
				x.NoError(<-closed)
				x.Equal([]int{1}, drain(blk.C))
				x.NoError(a.Close())
				x.NoError(blk.Close())
			})
		}
	})
	t.Run("a Block subscriber released by Close is ended, whatever else Close waits on", func(t *testing.T) {
		for range 10 { // a before b, or b before a
			x := require.New(t)
			// Key x of another Group, whose Block subscriber has stopped
			// reading. a's delivery feeds x, so it holds a's lock for as long
			// as x is stalled, and Close waits on it before it can end b.
			var ex streamflight.Emitter[int]
			other := &streamflight.Group[string, int]{
				Source: func(_ string, e streamflight.Emitter[int]) (func() error, error) {
					ex = e
					return nil, nil
				},
			}
			reached := make(chan struct{})
			signal, err := other.SubscribeFunc("x", func(v int) {
				if v == 1 {
					close(reached)
				}
			})
			x.NoError(err)
			xStalled, err := other.Subscribe("x", streamflight.WithOverflow(streamflight.Block))
			x.NoError(err)
			ex.Emit(0) // xStalled is full

			var mu sync.Mutex
			emitters := map[string]streamflight.Emitter[int]{}
			g := &streamflight.Group[string, int]{
				Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
					mu.Lock()
					emitters[key] = e
					mu.Unlock()
					return nil, nil
				},
			}
			a, err := g.SubscribeFunc("a", func(v int) { ex.Emit(v) })
			x.NoError(err)
			blk, err := g.Subscribe("b", streamflight.WithOverflow(streamflight.Block))
			x.NoError(err)
			emitters["b"].Emit(1) // blk is full
			go emitters["a"].Emit(1)
			<-reached // a's delivery now waits on xStalled

			closed := make(chan error, 1)
			go func() { closed <- g.Close() }()
			x.Equal(0, emitters["b"].Emit(2), "released by Close")
			eventually(t, func() bool { return blk.Err() != nil },
				"b is ended while Close still waits on a, not left live and refused values")
			x.ErrorIs(blk.Err(), streamflight.ErrGroupClosed)
			x.Empty(closed, "Close still waits on a")

			x.Equal(0, <-xStalled.C) // x moves again, and so does Close
			returns(t, func() { err = <-closed })
			x.NoError(err)
			x.Equal([]int{1}, drain(blk.C))
			for _, s := range []*streamflight.Subscription[int]{a, blk, signal, xStalled} {
				x.NoError(s.Close())
			}
			x.NoError(other.Close())
		}
	})
	t.Run("no shape of derived keys hangs Close on a stalled Block subscriber", func(t *testing.T) {
		// Each key but raw feeds on the key named, and its stop closes that
		// subscription. The stalled Block subscriber is on the key named last.
		shapes := []struct {
			name  string
			feeds map[string]string
			stall string
		}{
			{"two derived keys", map[string]string{"d1": "raw", "d2": "raw"}, "raw"},
			{"a chain, stalled at its root", map[string]string{"mid": "raw", "top": "mid"}, "raw"},
			{"a chain, stalled in its middle", map[string]string{"mid": "raw", "top": "mid"}, "mid"},
		}
		for _, shape := range shapes {
			t.Run(shape.name, func(t *testing.T) {
				for range 10 { // keys come up in any order
					x := require.New(t)
					var mu sync.Mutex
					emitters := map[string]streamflight.Emitter[int]{}
					g := &streamflight.Group[string, int]{}
					g.Source = func(key string, e streamflight.Emitter[int]) (func() error, error) {
						mu.Lock()
						emitters[key] = e
						mu.Unlock()
						from, ok := shape.feeds[key]
						if !ok {
							return nil, nil
						}
						sub, err := g.SubscribeFunc(from, func(v int) { e.Emit(v) })
						if err != nil {
							return nil, err
						}
						return sub.Close, nil
					}

					var subs []*streamflight.Subscription[int]
					for key := range shape.feeds {
						s, err := g.SubscribeFunc(key, func(int) {})
						x.NoError(err)
						subs = append(subs, s)
					}
					reached := make(chan struct{})
					signal, err := g.SubscribeFunc(shape.stall, func(v int) {
						if v == 2 {
							close(reached)
						}
					})
					x.NoError(err)
					stalled, err := g.Subscribe(shape.stall, streamflight.WithOverflow(streamflight.Block))
					x.NoError(err)
					mu.Lock()
					e := emitters[shape.stall]
					mu.Unlock()
					e.Emit(1) // stalled is full
					go e.Emit(2)
					<-reached // and this delivery waits on it

					returns(t, func() { err = g.Close() })
					x.NoError(err)
					for _, s := range append(subs, signal, stalled) {
						x.NoError(s.Close())
					}
				}
			})
		}
	})
	t.Run("a stop that closes a subscription to a key with a stalled Block subscriber does not hang", func(t *testing.T) {
		// derived is fed by a subscription to raw, which its stop closes. raw
		// has a Block subscriber that stopped reading, so raw's delivery holds
		// raw's lock. Stopping derived first used to wait on that lock, and
		// only stopping raw would have released it.
		for range 10 { // either key can come first
			x := require.New(t)
			var raw streamflight.Emitter[int]
			reached := make(chan struct{})
			g := &streamflight.Group[string, int]{}
			g.Source = func(key string, e streamflight.Emitter[int]) (func() error, error) {
				if key == "raw" {
					raw = e
					return nil, nil
				}
				sub, err := g.SubscribeFunc("raw", func(v int) { e.Emit(v) })
				if err != nil {
					return nil, err
				}
				return sub.Close, nil
			}

			d, err := g.SubscribeFunc("derived", func(v int) {
				if v == 2 {
					close(reached)
				}
			})
			x.NoError(err)
			stalled, err := g.Subscribe("raw", streamflight.WithOverflow(streamflight.Block))
			x.NoError(err)
			raw.Emit(1) // fills stalled's queue
			go raw.Emit(2)
			<-reached                         // derived has 2; stalled is next
			time.Sleep(10 * time.Millisecond) // and raw's delivery waits on it

			returns(t, func() { err = g.Close() })
			x.NoError(err)
			x.ErrorIs(stalled.Err(), streamflight.ErrGroupClosed)
			x.NoError(d.Close())
			x.NoError(stalled.Close())
		}
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
	t.Run("concurrent subscribers share an upstream that ends as it opens", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			boom := errors.New("connection refused")
			gate := make(chan struct{})
			opens, stops := 0, 0
			g := &streamflight.Group[string, int]{
				// As a Run whose function fails at once does, but in order.
				Source: func(_ string, e streamflight.Emitter[int]) (func() error, error) {
					opens++
					<-gate
					e.End(boom)
					return func() error { stops++; return nil }, nil
				},
			}

			const n = 8
			errs, ended := make([]error, n), make([]error, n)
			var wg sync.WaitGroup
			for i := range errs {
				wg.Go(func() {
					var s *streamflight.Subscription[int]
					if s, errs[i] = g.Subscribe("k"); s != nil {
						ended[i] = s.Err()
						s.Close() // at once, before the others have joined
					}
				})
			}
			synctest.Wait() // one is in the Source, the rest wait on it

			close(gate)
			wg.Wait()
			for i := range errs {
				x.NoError(errs[i])
				x.ErrorIs(ended[i], boom, "every one of them learns why it ended")
			}
			x.Equal(1, opens, "one attempt, shared by all of them, as a failed open is")
			x.Equal(1, stops)

			// One that comes afterwards opens it again, as after any end.
			s, err := g.Subscribe("k")
			x.NoError(err)
			x.NoError(s.Close())
			x.Equal(2, opens)
			x.NoError(g.Close())
		})
	})
	t.Run("one arriving after an End, while the Source still runs, shares it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			boom := errors.New("connection refused")
			ended, gate := make(chan struct{}), make(chan struct{})
			opens, stops := 0, 0
			g := &streamflight.Group[string, int]{
				Source: func(_ string, e streamflight.Emitter[int]) (func() error, error) {
					if opens++; opens == 1 {
						e.End(boom)
						close(ended)
						<-gate // the rest of a slow open
					}
					return func() error { stops++; return nil }, nil
				},
			}

			var first, late *streamflight.Subscription[int]
			var err error
			var wg sync.WaitGroup
			wg.Go(func() { first = must(g.Subscribe("k")) })
			<-ended
			wg.Go(func() { late, err = g.Subscribe("k") })
			synctest.Wait() // it waits on the open, End having returned
			close(gate)
			wg.Wait()

			x.NoError(err)
			x.ErrorIs(late.Err(), boom, "not the next subscriber: it shares the end")
			x.Equal(1, opens)
			x.NoError(first.Close())
			x.Zero(stops, "its arrival did not stop the upstream")
			x.NoError(late.Close())
			x.Equal(1, stops, "the last of them to close did")
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

func TestSubscribeContext(t *testing.T) {
	// gated is a Source that holds each open until its gate is closed.
	gated := func(r *recorder) (streamflight.Source[string, int], chan struct{}) {
		gate := make(chan struct{})
		return func(key string, e streamflight.Emitter[int]) (func() error, error) {
			<-gate
			return r.Source(key, e)
		}, gate
	}

	t.Run("a context already done opens nothing", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := g.SubscribeContext(ctx, "k")
		x.ErrorIs(err, context.Canceled)
		_, err = g.SubscribeFuncContext(ctx, "k", func(int) {})
		x.ErrorIs(err, context.Canceled)
		_, err = g.SubscribeLatestContext(ctx, "k")
		x.ErrorIs(err, context.Canceled)
		x.Empty(r.Log())
	})
	t.Run("it gives up waiting for another goroutine's open, which goes on", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			src, gate := gated(r)
			joins := 0
			g := &streamflight.Group[string, int]{
				Source: src,
				Hooks:  streamflight.Hooks[string, int]{Joined: func(string, int) { joins++ }},
			}

			opener := make(chan *streamflight.Subscription[int], 1)
			go func() { opener <- must(g.Subscribe("k")) }()
			synctest.Wait() // the opener is inside the Source

			ctx, cancel := context.WithCancel(t.Context())
			gaveUp := make(chan error, 1)
			go func() {
				_, err := g.SubscribeFuncContext(ctx, "k", func(int) {})
				gaveUp <- err
			}()
			synctest.Wait()
			cancel()
			x.ErrorIs(<-gaveUp, context.Canceled, "without waiting for the Source")

			close(gate)
			s := <-opener
			x.Equal(1, joins, "only the opener joined")
			x.NoError(s.Close())
			x.Equal([]string{"open k", "stop k"}, r.Log())
		})
	})
	t.Run("with Linger, giving up on an open arms no timer for it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			src, gate := gated(r)
			g := &streamflight.Group[string, int]{Source: src, Linger: time.Hour}

			opener := make(chan *streamflight.Subscription[int], 1)
			go func() { opener <- must(g.Subscribe("k")) }()
			synctest.Wait() // the opener is inside the Source

			ctx, cancel := context.WithCancel(t.Context())
			gaveUp := make(chan error, 1)
			go func() {
				_, err := g.SubscribeContext(ctx, "k")
				gaveUp <- err
			}()
			synctest.Wait()
			cancel()
			x.ErrorIs(<-gaveUp, context.Canceled)

			// Nobody was subscribed as it gave up, but the key was still
			// opening: a timer armed then would stop it under the opener.
			close(gate)
			s := <-opener
			time.Sleep(2 * time.Hour)
			synctest.Wait()
			x.Equal([]string{"open k"}, r.Log(), "the opener keeps it")
			x.Equal(1, r.emit("k", 1), "delivered to the opener")
			x.NoError(s.Close())
			time.Sleep(2 * time.Hour)
			synctest.Wait()
			x.Equal([]string{"open k", "stop k"}, r.Log(), "until it has lingered")
		})
	})
	t.Run("the opener that gives up during its Source gives back what it opened", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			src, gate := gated(r)
			// It gives it back as a last subscriber leaving would, but what it
			// opened is not stopped while the subscriber waiting on the same
			// open has yet to join it. Without Linger, so that nothing but that
			// wait keeps it running.
			g := &streamflight.Group[string, int]{Source: src}

			ctx, cancel := context.WithCancel(t.Context())
			gaveUp := make(chan error, 1)
			go func() {
				_, err := g.SubscribeLatestContext(ctx, "k")
				gaveUp <- err
			}()
			synctest.Wait() // inside the Source, which takes no context
			waiter := make(chan *streamflight.Subscription[int], 1)
			go func() { waiter <- must(g.Subscribe("k")) }()
			synctest.Wait()

			cancel()
			synctest.Wait()
			x.Empty(gaveUp, "still in the Source")
			close(gate)
			x.ErrorIs(<-gaveUp, context.Canceled, "once the Source returned")

			s := <-waiter
			x.Equal([]string{"open k"}, r.Log(), "the waiter keeps what was opened")
			x.NoError(s.Close())
			x.Equal([]string{"open k", "stop k"}, r.Log(), "and nothing is left held")
		})
	})
	t.Run("an opener that gives up hands what it opened to those waiting on it, without Linger", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			src, gate := gated(r)
			g := &streamflight.Group[string, int]{Source: src}

			ctx, cancel := context.WithCancel(t.Context())
			gaveUp := make(chan error, 1)
			go func() {
				_, err := g.SubscribeContext(ctx, "k")
				gaveUp <- err
			}()
			synctest.Wait() // the opener is inside the Source
			waiters := make(chan *streamflight.Subscription[int], 2)
			go func() { waiters <- must(g.Subscribe("k")) }()
			other, otherCancel := context.WithTimeout(t.Context(), time.Hour)
			defer otherCancel()
			go func() { waiters <- must(g.SubscribeFuncContext(other, "k", func(int) {})) }()
			synctest.Wait() // and two wait on that open

			cancel()
			close(gate)
			x.ErrorIs(<-gaveUp, context.Canceled)
			a, b := <-waiters, <-waiters
			x.Equal([]string{"open k"}, r.Log(), "joined, not stopped and opened again")
			x.NoError(a.Close())
			x.NoError(b.Close())
			x.Equal([]string{"open k", "stop k"}, r.Log())
		})
	})
	t.Run("once every waiter has given up too, what was opened is stopped", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			src, gate := gated(r)
			g := &streamflight.Group[string, int]{Source: src}

			ctx, cancel := context.WithCancel(t.Context())
			errs := make(chan error, 3)
			for range 3 { // the opener first, then two waiters
				go func() {
					_, err := g.SubscribeContext(ctx, "k")
					errs <- err
				}()
				synctest.Wait()
			}
			cancel()
			for range 2 {
				x.ErrorIs(<-errs, context.Canceled, "the waiters give up at once")
			}
			close(gate)
			x.ErrorIs(<-errs, context.Canceled, "the opener once its Source returns")
			synctest.Wait()
			x.Equal([]string{"open k", "stop k"}, r.Log(), "nobody was left to hand it to")
		})
	})
	t.Run("without Linger, an opener that gives up leaves nothing held either", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			src, gate := gated(r)
			g := &streamflight.Group[string, int]{Source: src}

			ctx, cancel := context.WithCancel(t.Context())
			gaveUp := make(chan error, 1)
			go func() {
				_, err := g.SubscribeContext(ctx, "k")
				gaveUp <- err
			}()
			synctest.Wait()
			waiter := make(chan *streamflight.Subscription[int], 1)
			go func() { waiter <- must(g.Subscribe("k", streamflight.WithBuffer(4))) }()
			synctest.Wait()

			cancel()
			close(gate)
			x.ErrorIs(<-gaveUp, context.Canceled)
			s := <-waiter
			r.emitter("k").Emit(1)
			x.Equal(1, <-s.C, "the waiter has a working subscription, on what was opened")
			x.NoError(s.Close())
			x.Equal([]string{"open k", "stop k"}, r.Log(), "opened once, and stopped")
		})
	})
	t.Run("it gives up waiting on a delivery a Block subscriber holds up", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		reached := make(chan struct{})
		first, err := g.SubscribeFunc("k", func(v int) {
			if v == 2 {
				close(reached)
			}
		})
		x.NoError(err)
		stalled, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
		x.NoError(err)
		r.emit("k", 1)
		emitted := make(chan int, 1)
		go func() { emitted <- r.emit("k", 2) }()
		<-reached
		time.Sleep(10 * time.Millisecond) // the delivery now waits on stalled

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err = g.SubscribeContext(ctx, "k")
		x.ErrorIs(err, context.DeadlineExceeded)

		x.Equal(1, <-stalled.C) // make room: the delivery ends
		x.Equal(2, <-emitted)
		x.Equal(2, <-stalled.C)
		eventually(t, func() bool { return !helping() }, "the lock it queued for is let go")
		var n int
		returns(t, func() { n = r.emit("k", 3) })
		x.Equal(2, n, "nobody joined, and the key's lock is free")
		x.NoError(first.Close())
		x.NoError(stalled.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log(), "its reference was given back")
	})
	t.Run("it joins once the delivery it waited on is done, each time", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		seen := make(chan int, 16)
		first, err := g.SubscribeFunc("k", func(v int) { seen <- v })
		x.NoError(err)
		stalled, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
		x.NoError(err)

		var joined []*streamflight.Subscription[int]
		for round := range 2 { // the lock is handed over again, for the next
			fill, held := 2*round+1, 2*round+2
			r.emit("k", fill) // stalled is full
			x.Equal(fill, <-seen)
			go r.emit("k", held)
			x.Equal(held, <-seen) // this delivery now waits on stalled, holding the lock

			join := make(chan *streamflight.Subscription[int], 1)
			go func() { join <- must(g.SubscribeContext(t.Context(), "k", streamflight.WithBuffer(4))) }()
			eventually(t, helping, "it queues for the lock")
			x.Equal(fill, <-stalled.C)
			var s *streamflight.Subscription[int]
			returns(t, func() { s = <-join })
			x.Equal(held, <-stalled.C)
			eventually(t, func() bool { return !helping() }, "the lock is let go")
			joined = append(joined, s)
		}
		r.emit("k", 9)
		x.Equal([]int{3, 4, 9}, drain(joined[0].C))
		x.Equal([]int{9}, drain(joined[1].C))

		for _, s := range append(joined, first, stalled) {
			x.NoError(s.Close())
		}
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("callers that keep giving up on a held key cost it one goroutine", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		reached := make(chan struct{})
		first, err := g.SubscribeFunc("k", func(v int) {
			if v == 2 {
				close(reached)
			}
		})
		x.NoError(err)
		stalled, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
		x.NoError(err)
		r.emit("k", 1)
		emitted := make(chan int, 1)
		go func() { emitted <- r.emit("k", 2) }()
		<-reached // the delivery holds the key's lock until stalled reads

		before := runtime.NumGoroutine()
		for range 200 {
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Microsecond)
			_, err := g.SubscribeContext(ctx, "k")
			cancel()
			x.ErrorIs(err, context.DeadlineExceeded)
		}
		x.Less(runtime.NumGoroutine(), before+5, "one goroutine waits for the key, not one per caller")
		x.True(helping())

		x.Equal(1, <-stalled.C)
		x.Equal(2, <-emitted)
		x.Equal(2, <-stalled.C)
		eventually(t, func() bool { return !helping() }, "it lets the key go once nobody waits")
		var n int
		returns(t, func() { n = r.emit("k", 3) })
		x.Equal(2, n, "none of them joined")
		x.NoError(first.Close())
		x.NoError(stalled.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("a caller that gives up as it is handed the lock does not keep it", func(t *testing.T) {
		x := require.New(t)
		// With one P every caller has given up before help takes the lock.
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(max(2, runtime.GOMAXPROCS(0))))
		for range 50 { // some give up after they are taken off the queue
			r := newRecorder()
			g := &streamflight.Group[string, int]{Source: r.Source}
			reached := make(chan struct{})
			first, err := g.SubscribeFunc("k", func(v int) {
				if v == 2 {
					close(reached)
				}
			})
			x.NoError(err)
			stalled, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
			x.NoError(err)
			r.emit("k", 1)
			go r.emit("k", 2)
			<-reached // the delivery of 2 holds the key's lock until stalled reads

			ctx, cancel := context.WithCancel(t.Context())
			var wg sync.WaitGroup
			var mu sync.Mutex
			var joined []*streamflight.Subscription[int]
			for range 8 {
				wg.Go(func() {
					if s, err := g.SubscribeContext(ctx, "k"); err == nil {
						mu.Lock()
						joined = append(joined, s)
						mu.Unlock()
					}
				})
			}
			eventually(t, func() bool { return running(").lock(") == 8 }, "the callers are queued")
			cancel()
			x.Equal(1, <-stalled.C) // the lock is let go as they give up
			returns(t, wg.Wait)
			x.Equal(2, <-stalled.C)
			returns(t, func() { r.emit("k", 3) }) // nobody who gave up is left holding it

			for _, s := range joined {
				x.NoError(s.Close())
			}
			x.NoError(first.Close())
			x.NoError(stalled.Close())
			x.Equal([]string{"open k", "stop k"}, r.Log())
		}
	})
	t.Run("callers queued for a held key give up in any order", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		reached := make(chan struct{})
		first, err := g.SubscribeFunc("k", func(v int) {
			if v == 2 {
				close(reached)
			}
		})
		x.NoError(err)
		stalled, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
		x.NoError(err)
		r.emit("k", 1)
		emitted := make(chan int, 1)
		go func() { emitted <- r.emit("k", 2) }()
		<-reached

		// Queued in this order; the middle one gives up first, then the last,
		// then the first.
		after := []time.Duration{300 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}
		errs := make(chan error, len(after))
		for _, d := range after {
			go func() {
				ctx, cancel := context.WithTimeout(t.Context(), d)
				defer cancel()
				_, err := g.SubscribeContext(ctx, "k")
				errs <- err
			}()
			time.Sleep(20 * time.Millisecond)
		}
		for range after {
			x.ErrorIs(<-errs, context.DeadlineExceeded)
		}

		x.Equal(1, <-stalled.C)
		x.Equal(2, <-emitted)
		eventually(t, func() bool { return !helping() }, "the lock is let go once nobody is queued")
		x.NoError(first.Close())
		x.NoError(stalled.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("on a key whose lock is free it costs what Subscribe does", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{Source: newRecorder().Source}
		keep, err := g.SubscribeFunc("k", func(int) {})
		x.NoError(err)
		defer keep.Close()

		ctx := t.Context()
		plain := testing.AllocsPerRun(100, func() { must(g.Subscribe("k")).Close() })
		withCtx := testing.AllocsPerRun(100, func() { must(g.SubscribeContext(ctx, "k")).Close() })
		x.Equal(plain, withCtx, "no queue and no goroutine unless the lock is held")
	})
	t.Run("it gives up waiting for another goroutine's stop", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			gate := make(chan struct{})
			opens := 0
			g := &streamflight.Group[string, int]{
				Source: func(string, streamflight.Emitter[int]) (func() error, error) {
					opens++
					return func() error { <-gate; return nil }, nil
				},
			}

			s, err := g.Subscribe("k")
			x.NoError(err)
			go s.Close()
			synctest.Wait() // the stop is running

			ctx, cancel := context.WithCancel(t.Context())
			gaveUp := make(chan error, 1)
			go func() {
				_, err := g.SubscribeContext(ctx, "k")
				gaveUp <- err
			}()
			synctest.Wait()
			x.Empty(gaveUp, "waiting for the stop")
			cancel()
			x.ErrorIs(<-gaveUp, context.Canceled, "before the stop returned")

			close(gate)
			synctest.Wait()
			x.Equal(1, opens, "it opened nothing")
		})
	})
	t.Run("it gives up after a stop it ran itself, rather than open afresh", func(t *testing.T) {
		x := require.New(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		opens := 0
		var e streamflight.Emitter[int]
		g := &streamflight.Group[string, int]{
			Source: func(_ string, e_ streamflight.Emitter[int]) (func() error, error) {
				opens++
				e = e_
				return func() error {
					cancel() // ctx ends while it runs this stop
					return nil
				}, nil
			},
		}

		s, err := g.Subscribe("k")
		x.NoError(err)
		e.End(nil) // it ended, and s still holds it: the next subscriber stops it
		_, err = g.SubscribeContext(ctx, "k")
		x.ErrorIs(err, context.Canceled)
		x.Equal(1, opens)
		x.NoError(s.Close())
	})
	t.Run("an open that fails as it gives up returns the open's error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			boom := errors.New("boom")
			gate := make(chan struct{})
			g := &streamflight.Group[string, int]{
				Source: func(string, streamflight.Emitter[int]) (func() error, error) {
					<-gate
					return nil, boom
				},
			}

			ctx, cancel := context.WithCancel(t.Context())
			res := make(chan error, 1)
			go func() {
				_, err := g.SubscribeContext(ctx, "k")
				res <- err
			}()
			synctest.Wait() // it is the one opening
			cancel()
			close(gate)
			x.ErrorIs(<-res, boom, "as Subscribe would")
		})
	})
	t.Run("each Context variant makes the kind of subscription its name says", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}
		ctx := t.Context()

		ch, err := g.SubscribeContext(ctx, "k", streamflight.WithBuffer(4))
		x.NoError(err)
		var got collector
		fn, err := g.SubscribeFuncContext(ctx, "k", got.Add)
		x.NoError(err)
		l, err := g.SubscribeLatestContext(ctx, "k")
		x.NoError(err)

		r.emit("k", 5)
		x.Equal([]int{5}, drain(ch.C))
		x.Equal([]int{5}, got.Values())
		x.Nil(fn.C)
		x.Nil(l.C)
		v, _, ok := l.Latest()
		x.True(ok)
		x.Equal(5, v)
		x.Panics(func() { fn.Latest() }, "a function subscriber, not a sampler")
		for _, s := range []*streamflight.Subscription[int]{ch, fn, l} {
			x.NoError(s.Close())
		}
	})
	t.Run("under churn, every reference is given back and every open stopped", func(t *testing.T) {
		x := require.New(t)
		var mu sync.Mutex
		opens, stops, joins, leaves := 0, 0, 0, 0
		g := &streamflight.Group[string, int]{
			Source: func(string, streamflight.Emitter[int]) (func() error, error) {
				mu.Lock()
				opens++
				mu.Unlock()
				time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond)
				return func() error {
					mu.Lock()
					stops++
					mu.Unlock()
					return nil
				}, nil
			},
			Hooks: streamflight.Hooks[string, int]{
				Joined: func(string, int) { joins++ }, // under the Group's lock
				Left:   func(string, int) { leaves++ },
			},
		}

		before := runtime.NumGoroutine()
		var wg sync.WaitGroup
		for i := range 16 {
			wg.Go(func() {
				for j := range 200 {
					key := fmt.Sprint("k", (i+j)%3)
					ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rand.IntN(300))*time.Microsecond)
					var s *streamflight.Subscription[int]
					var err error
					switch j % 3 {
					case 0:
						s, err = g.SubscribeContext(ctx, key, streamflight.WithOverflow(streamflight.Block))
					case 1:
						s, err = g.SubscribeFuncContext(ctx, key, func(int) {})
					default:
						s, err = g.Subscribe(key)
					}
					cancel()
					if err == nil {
						s.Close()
					} else if !errors.Is(err, context.DeadlineExceeded) {
						t.Error(err)
					}
				}
			})
		}
		wg.Wait()
		// Linger is zero and every subscription has been closed, so an
		// upstream still running here is held by a caller that never gave its
		// place back. Close would stop it all the same.
		mu.Lock()
		opened, stopped := opens, stops
		mu.Unlock()
		x.Equal(opened, stopped, "every upstream was stopped by whoever left it last")
		x.NoError(g.Close())
		x.Equal(opens, stops, "and Close found nothing left to stop")
		x.Equal(joins, leaves, "every Joined was followed by a Left")
		eventually(t, func() bool { return runtime.NumGoroutine() <= before }, "no goroutine is left behind")
	})
	t.Run("Close with Context callers queued for a held key", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		reached := make(chan struct{})
		first, err := g.SubscribeFunc("k", func(v int) {
			if v == 2 {
				close(reached)
			}
		})
		x.NoError(err)
		stalled, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Block))
		x.NoError(err)
		r.emit("k", 1)
		go r.emit("k", 2)
		<-reached

		type joining struct {
			s   *streamflight.Subscription[int]
			err error
		}
		joined := make(chan joining, 3)
		for range 3 {
			go func() {
				s, err := g.SubscribeContext(t.Context(), "k")
				joined <- joining{s, err}
			}()
		}
		// All three, not only the first: one not queued yet would find the
		// Group closed.
		eventually(t, func() bool { return running(").lock(") == 3 }, "they queue for the lock")
		returns(t, func() { err = g.Close() })
		x.NoError(err)
		for range 3 {
			var j joining
			returns(t, func() { j = <-joined })
			x.NoError(j.err, "queued before Close")
			x.ErrorIs(j.s.Err(), streamflight.ErrGroupClosed, "handed the lock, then found the key ended")
			x.NoError(j.s.Close())
		}
		eventually(t, func() bool { return !helping() }, "and the lock is let go")
		x.NoError(first.Close())
		x.NoError(stalled.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log())
	})
	t.Run("once it has returned, the context has no effect", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{Source: r.Source}

		ctx, cancel := context.WithCancel(t.Context())
		s, err := g.SubscribeContext(ctx, "k", streamflight.WithBuffer(4))
		x.NoError(err)
		cancel()
		r.emit("k", 1)
		x.Equal(1, <-s.C)
		x.NoError(s.Err())
		x.NoError(s.Close())
	})
	t.Run("a nil context panics", func(t *testing.T) {
		x := require.New(t)
		g := &streamflight.Group[string, int]{Source: newRecorder().Source}
		var nilCtx context.Context

		const msg = "streamflight: nil Context"
		x.PanicsWithValue(msg, func() { g.SubscribeContext(nilCtx, "k") })
		x.PanicsWithValue(msg, func() { g.SubscribeFuncContext(nilCtx, "k", func(int) {}) })
		x.PanicsWithValue(msg, func() { g.SubscribeLatestContext(nilCtx, "k") })
	})
}

func TestEndedAndEvicted(t *testing.T) {
	// What Stopped and Dropped do not tell apart: an upstream that failed from
	// one the Group stopped, and a subscriber cut off from a value refused.
	newGroup := func(r *recorder, log *[]string) *streamflight.Group[string, int] {
		return &streamflight.Group[string, int]{
			Source: r.Source,
			Replay: 3,
			Hooks: streamflight.Hooks[string, int]{
				Stopped: func(key string, err error) { *log = append(*log, fmt.Sprintf("stopped %s %v", key, err)) },
				Dropped: func(key string, v int) { *log = append(*log, fmt.Sprintf("dropped %s %d", key, v)) },
				Evicted: func(key string) { *log = append(*log, "evicted "+key) },
				Ended:   func(key string, err error) { *log = append(*log, fmt.Sprintf("ended %s %v", key, err)) },
			},
		}
	}

	t.Run("Evicted reports each subscriber Evict cuts off, live or catching up", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var log []string
		g := newGroup(r, &log)

		evict := streamflight.WithOverflow(streamflight.Evict)
		a, err := g.Subscribe("k", evict)
		x.NoError(err)
		b, err := g.Subscribe("k", evict)
		x.NoError(err)
		r.emit("k", 1, 2)
		x.Equal([]string{"evicted k", "evicted k"}, log, "and no Dropped for the value they had no room for")

		late, err := g.Subscribe("k", evict)
		x.NoError(err)
		x.ErrorIs(late.Err(), streamflight.ErrEvicted)
		x.Equal([]string{"evicted k", "evicted k", "evicted k"}, log, "the catch-up did not fit")

		for _, s := range []*streamflight.Subscription[int]{a, b, late} {
			x.NoError(s.Close())
		}
	})
	t.Run("Ended reports an upstream that ends by itself, before Stopped", func(t *testing.T) {
		x := require.New(t)
		boom := errors.New("boom")
		r := newRecorder()
		var log []string
		g := newGroup(r, &log)

		s, err := g.Subscribe("k")
		x.NoError(err)
		r.emitter("k").End(boom)
		x.NoError(s.Close())

		s, err = g.Subscribe("k")
		x.NoError(err)
		r.emitter("k").End(nil)
		x.NoError(s.Close())

		s, err = g.Subscribe("k")
		x.NoError(err)
		x.NoError(s.Close())
		r.emitter("k").End(boom) // after the Group stopped it: nothing to report

		x.Equal([]string{
			"ended k boom", "stopped k <nil>",
			"ended k EOF", "stopped k <nil>",
			"stopped k <nil>",
		}, log)
	})
	t.Run("Ended is not reported for an upstream the Group stops", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var log []string
		g := newGroup(r, &log)

		s, err := g.Subscribe("k")
		x.NoError(err)
		x.NoError(g.Close())
		r.emitter("k").End(nil) // too late: it was stopped
		x.ErrorIs(s.Err(), streamflight.ErrGroupClosed)
		x.Equal([]string{"stopped k <nil>"}, log)
	})
	t.Run("Ended is not reported for a key Close stops, even one that ends itself in reply", func(t *testing.T) {
		for range 10 { // raw ended first, or derived
			x := require.New(t)
			var mu sync.Mutex
			var ended []string
			g := &streamflight.Group[string, int]{
				Hooks: streamflight.Hooks[string, int]{
					Ended: func(key string, err error) {
						mu.Lock()
						ended = append(ended, key)
						mu.Unlock()
					},
				},
			}
			g.Source = func(key string, e streamflight.Emitter[int]) (func() error, error) {
				if key == "raw" {
					return nil, nil
				}
				// derived ends when raw does, as a stream fed by another would.
				sub, err := g.Subscribe("raw")
				if err != nil {
					return nil, err
				}
				go func() {
					<-sub.Done()
					e.End(sub.Err())
				}()
				return sub.Close, nil
			}

			d, err := g.Subscribe("derived")
			x.NoError(err)
			x.NoError(g.Close())
			<-d.Done()
			time.Sleep(time.Millisecond) // for the End that replies to raw's end
			mu.Lock()
			x.Empty(ended, "Close stopped both; neither ended by itself")
			mu.Unlock()
			x.NoError(d.Close())
		}
	})
	t.Run("Ended is not reported for a Run or Poll upstream the Group stops", func(t *testing.T) {
		x := require.New(t)
		var ended, stopped atomic.Int64
		hooks := streamflight.Hooks[string, int]{
			Ended:   func(string, error) { ended.Add(1) },
			Stopped: func(string, error) { stopped.Add(1) },
		}
		sources := []streamflight.Source[string, int]{
			streamflight.Run(func(ctx context.Context, _ string, _ streamflight.Emitter[int]) error {
				<-ctx.Done()
				return ctx.Err() // returned only because the key is stopping
			}),
			streamflight.Poll(time.Hour, func(context.Context, string, streamflight.Emitter[int]) error {
				return nil
			}),
		}
		for _, src := range sources {
			for _, byClose := range []bool{false, true} {
				g := &streamflight.Group[string, int]{Source: src, Hooks: hooks}
				s, err := g.Subscribe("k")
				x.NoError(err)
				if byClose {
					x.NoError(g.Close())
				}
				x.NoError(s.Close())
			}
		}
		x.Zero(ended.Load())
		x.Equal(int64(4), stopped.Load())
	})
	t.Run("a Run upstream a Source stops itself ends with what run returns", func(t *testing.T) {
		x := require.New(t)
		var hooks []string
		setup := errors.New("setup failed")
		g := &streamflight.Group[string, int]{
			// As a Source whose setup after Run fails must: stop what Run
			// started, which the Group is not stopping.
			Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
				stop, _ := streamflight.Run(func(ctx context.Context, _ string, _ streamflight.Emitter[int]) error {
					<-ctx.Done()
					return ctx.Err()
				})(key, e)
				x.NoError(stop())
				return nil, setup
			},
			Hooks: streamflight.Hooks[string, int]{
				Ended:   func(_ string, err error) { hooks = append(hooks, "ended: "+err.Error()) },
				Opened:  func(_ string, err error) { hooks = append(hooks, "opened: "+err.Error()) },
				Stopped: func(string, error) { t.Error("nothing opened to stop") },
			},
		}

		_, err := g.Subscribe("k")
		x.ErrorIs(err, setup)
		x.Equal([]string{"ended: context canceled", "opened: setup failed"}, hooks)
	})
	t.Run("an End racing Close ends every subscriber with one reason", func(t *testing.T) {
		for range 200 {
			x := require.New(t)
			boom := errors.New("boom")
			r := newRecorder()
			var ended atomic.Int64
			g := &streamflight.Group[string, int]{
				Source: r.Source,
				Hooks:  streamflight.Hooks[string, int]{Ended: func(string, error) { ended.Add(1) }},
			}
			var subs []*streamflight.Subscription[int]
			for range 3 {
				s, err := g.Subscribe("k")
				x.NoError(err)
				subs = append(subs, s)
			}
			var wg sync.WaitGroup
			wg.Go(func() { r.emitter("k").End(boom) })
			wg.Go(func() { g.Close() })
			wg.Wait()

			reason := subs[0].Err()
			for _, s := range subs {
				x.Equal(reason, s.Err(), "one reason for all")
				x.NoError(s.Close())
			}
			x.True(errors.Is(reason, boom) || errors.Is(reason, streamflight.ErrGroupClosed), reason)
			if ended.Load() > 0 {
				// At most once, and then with the reason its subscribers were
				// given. Whether End came before Close began cannot be seen
				// from here: the next subtest pins that.
				x.Equal(int64(1), ended.Load())
				x.ErrorIs(reason, boom)
			}
		}
	})
	t.Run("Ended is not reported for an End that comes once Close has begun", func(t *testing.T) {
		for range 10 {
			x := require.New(t)
			r := newRecorder()
			var ended atomic.Int64
			g := &streamflight.Group[string, int]{
				Source: r.Source,
				Hooks:  streamflight.Hooks[string, int]{Ended: func(string, error) { ended.Add(1) }},
			}
			inFn, release := make(chan struct{}), make(chan struct{})
			s, err := g.SubscribeFunc("k", func(int) { close(inFn); <-release })
			x.NoError(err)
			other, err := g.Subscribe("other")
			x.NoError(err)

			emitted := make(chan struct{})
			go func() {
				r.emit("k", 1) // holds k's lock until released
				r.emitter("k").End(errors.New("boom"))
				close(emitted)
			}()
			<-inFn
			closed := make(chan error, 1)
			go func() { closed <- g.Close() }()
			<-other.Done() // Close has begun stopping every key, k included
			close(release) // the End comes now, and may take k's lock first
			<-emitted
			x.NoError(<-closed)
			x.Zero(ended.Load(), "the Group was stopping k by then")
			x.NoError(s.Close())
			x.NoError(other.Close())
		}
	})
	t.Run("Ended comes before Stopped on every way a key is stopped", func(t *testing.T) {
		ways := map[string]func(g *streamflight.Group[string, int], s *streamflight.Subscription[int]){
			"the last subscriber leaving": func(_ *streamflight.Group[string, int], s *streamflight.Subscription[int]) { s.Close() },
			"the next subscriber taking over": func(g *streamflight.Group[string, int], _ *streamflight.Subscription[int]) {
				must(g.Subscribe("k")).Close()
			},
			"Close": func(g *streamflight.Group[string, int], _ *streamflight.Subscription[int]) { g.Close() },
		}
		for name, stop := range ways {
			t.Run(name, func(t *testing.T) {
				x := require.New(t)
				r := newRecorder()
				var mu sync.Mutex
				var log []string
				report := func(what string) {
					mu.Lock()
					log = append(log, what)
					mu.Unlock()
				}
				inEnded, release := make(chan struct{}), make(chan struct{})
				g := &streamflight.Group[string, int]{
					Source: r.Source,
					Hooks: streamflight.Hooks[string, int]{
						Ended: func(key string, _ error) {
							if len(r.Log()) == 1 { // the first upstream only
								close(inEnded)
								<-release
							}
							report("ended")
						},
						Stopped: func(string, error) { report("stopped") },
					},
				}
				s, err := g.Subscribe("k")
				x.NoError(err)
				go r.emitter("k").End(nil)
				<-inEnded
				stopped := make(chan struct{})
				go func() {
					stop(g, s)
					close(stopped)
				}()
				time.Sleep(10 * time.Millisecond)
				mu.Lock()
				x.Empty(log, "the stop waits for Ended")
				mu.Unlock()
				close(release)
				<-stopped
				mu.Lock()
				x.Equal([]string{"ended", "stopped"}, log[:2])
				mu.Unlock()
				s.Close()
				g.Close()
			})
		}
	})
	t.Run("Ended comes before Stopped even when another goroutine stops it", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var mu sync.Mutex
		var log []string
		inEnded, release := make(chan struct{}), make(chan struct{})
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Ended: func(string, error) {
					close(inEnded)
					<-release
					mu.Lock()
					log = append(log, "ended")
					mu.Unlock()
				},
				Stopped: func(string, error) {
					mu.Lock()
					log = append(log, "stopped")
					mu.Unlock()
				},
			},
		}

		s, err := g.Subscribe("k")
		x.NoError(err)
		go r.emitter("k").End(nil)
		<-inEnded
		closed := make(chan error, 1)
		go func() { closed <- s.Close() }() // the last subscriber leaving stops it
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		x.Empty(log, "the stop waits for Ended, which holds the key's lock")
		mu.Unlock()
		close(release)
		x.NoError(<-closed)
		x.Equal([]string{"ended", "stopped"}, log)
	})
}

func TestPanics(t *testing.T) {
	// A panic in the caller's code fails the call it ran in. It must not leave
	// the Group locked or anyone waiting on a key forever, and what the Source
	// opened must still be stoppable.
	const boom = "boom"

	t.Run("a Joined hook that panics on an open key takes no reference with it", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Joined: func(_ string, n int) {
					if n == 2 {
						panic(boom)
					}
				},
			},
		}

		a, err := g.Subscribe("k")
		x.NoError(err)
		x.PanicsWithValue(boom, func() { g.Subscribe("k") })

		var other *streamflight.Subscription[int]
		returns(t, func() { other, err = g.Subscribe("other") })
		x.NoError(err)
		x.NoError(other.Close())
		x.NoError(a.Close())
		x.Equal([]string{"open k", "open other", "stop other", "stop k"}, r.Log(),
			"a held the last reference to k")
	})
	t.Run("a Joined hook that panics while opening wakes whoever waits on the key", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			release := make(chan struct{})
			var once atomic.Bool
			g := &streamflight.Group[string, int]{
				Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
					<-release
					return r.Source(key, e)
				},
				Hooks: streamflight.Hooks[string, int]{
					Joined: func(string, int) {
						if once.CompareAndSwap(false, true) {
							panic(boom)
						}
					},
				},
			}

			panicked := make(chan any, 1)
			go func() {
				defer func() { panicked <- recover() }()
				g.Subscribe("k")
			}()
			synctest.Wait() // the opener is inside the Source
			type result struct {
				s   *streamflight.Subscription[int]
				err error
			}
			waiter := make(chan result, 1)
			go func() {
				s, err := g.Subscribe("k")
				waiter <- result{s, err}
			}()
			synctest.Wait() // and a second subscriber waits on the key

			close(release)
			x.Equal(boom, <-panicked)
			w := <-waiter
			x.NoError(w.err)
			x.Equal([]string{"open k"}, r.Log(), "it joined what was opened")
			x.NoError(w.s.Close())
			x.Equal([]string{"open k", "stop k"}, r.Log(), "and leaving stopped it")
		})
	})
	t.Run("an Opened hook that panics leaves what was opened stoppable", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var once atomic.Bool
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Opened: func(string, error) {
					if once.CompareAndSwap(false, true) {
						panic(boom)
					}
				},
			},
		}

		x.PanicsWithValue(boom, func() { g.Subscribe("k") })
		x.Equal([]string{"open k"}, r.Log())

		var err error
		returns(t, func() { err = g.Close() })
		x.NoError(err)
		x.Equal([]string{"open k", "stop k"}, r.Log(), "Close found it and stopped it")
	})
	t.Run("an Opened hook that panics leaves what was opened to the next subscriber to leave", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var once atomic.Bool
		var joined []int
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Opened: func(string, error) {
					if once.CompareAndSwap(false, true) {
						panic(boom)
					}
				},
				Joined: func(_ string, n int) { joined = append(joined, n) },
			},
		}

		x.PanicsWithValue(boom, func() { g.Subscribe("k") })
		s, err := g.Subscribe("k")
		x.NoError(err)
		x.Equal([]int{1}, joined, "the opener that panicked was never counted")
		x.NoError(s.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log(), "the next subscriber to leave stopped it")
	})
	t.Run("a Joined hook that panics on a lingering key leaves it to expire", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			var joins atomic.Int64
			g := &streamflight.Group[string, int]{
				Source: r.Source,
				Linger: time.Second,
				Hooks: streamflight.Hooks[string, int]{
					Joined: func(string, int) {
						if joins.Add(1) == 2 {
							panic(boom)
						}
					},
				},
			}

			a, err := g.Subscribe("k")
			x.NoError(err)
			x.NoError(a.Close())
			x.PanicsWithValue(boom, func() { g.Subscribe("k") })

			time.Sleep(2 * time.Second)
			synctest.Wait()
			x.Equal([]string{"open k", "stop k"}, r.Log(), "the linger timer still stops it")
		})
	})
	t.Run("a Left hook that panics leaves the upstream stoppable", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var once atomic.Bool
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Left: func(string, int) {
					if once.CompareAndSwap(false, true) {
						panic(boom)
					}
				},
			},
		}

		a, err := g.Subscribe("k")
		x.NoError(err)
		x.PanicsWithValue(boom, func() { a.Close() })

		var b *streamflight.Subscription[int]
		returns(t, func() { b, err = g.Subscribe("k") })
		x.NoError(err)
		x.Equal([]string{"open k"}, r.Log(), "still running, and joined")
		x.NoError(b.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log(), "the next to leave stopped it")
	})
	t.Run("an Initial that panics gives back its reference and refuses its send", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var once atomic.Bool
		var escaped func(int)
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Initial: func(_ string, send func(int)) {
				if once.CompareAndSwap(false, true) {
					escaped = send
					panic(boom)
				}
			},
		}

		x.PanicsWithValue(boom, func() { g.Subscribe("k") })
		x.Equal([]string{"open k", "stop k"}, r.Log(), "its reference was the only one")
		x.PanicsWithValue("streamflight: Group.Initial called send after returning", func() {
			escaped(1)
		})

		var s *streamflight.Subscription[int]
		var err error
		returns(t, func() { s, err = g.Subscribe("k") })
		x.NoError(err)
		x.NoError(s.Close())
	})
	t.Run("a Dropped hook that panics fails the Emit, not the key", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Dropped: func(string, int) { panic(boom) },
			},
		}

		s, err := g.Subscribe("k")
		x.NoError(err)
		r.emit("k", 1)
		x.PanicsWithValue(boom, func() { r.emit("k", 2) }, "1 is displaced")

		var n int
		returns(t, func() { n = r.emit("k", 3) })
		x.Equal(1, n)
		x.Equal([]int{3}, drain(s.C))
		returns(t, func() { err = s.Close() })
		x.NoError(err)
	})
	t.Run("a Now that panics fails the Emit, not the samplers", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var once atomic.Bool
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Now: func() time.Time {
				if once.CompareAndSwap(false, true) {
					panic(boom)
				}
				return time.Now()
			},
		}

		s, err := g.SubscribeLatest("k")
		x.NoError(err)
		x.PanicsWithValue(boom, func() { r.emit("k", 1) })

		var ok bool
		returns(t, func() { _, _, ok = s.Latest() })
		x.False(ok, "nothing was kept")
		r.emit("k", 2)
		v, _, ok := s.Latest()
		x.True(ok)
		x.Equal(2, v)
		x.NoError(s.Close())
	})
	t.Run("an Evicted hook that panics fails the Emit, and the evicted stay out", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Evicted: func(string) { panic(boom) },
			},
		}

		a, err := g.Subscribe("k", streamflight.WithOverflow(streamflight.Evict))
		x.NoError(err)
		keep, err := g.Subscribe("k", streamflight.WithBuffer(8))
		x.NoError(err)
		r.emit("k", 1)
		x.PanicsWithValue(boom, func() { r.emit("k", 2) })
		x.ErrorIs(a.Err(), streamflight.ErrEvicted)

		var n int
		returns(t, func() { n = r.emit("k", 3) })
		x.Equal(1, n, "a is gone, and nothing is sent to its closed queue")
		x.Equal([]int{1, 3}, drain(keep.C), "2 never got past the panic")
		x.NoError(a.Close())
		x.NoError(keep.Close())
	})
	t.Run("an Evicted hook that panics on a later eviction of one pass leaves nobody ended in", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var evictions atomic.Int64
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Evicted: func(string) {
					if evictions.Add(1) == 2 {
						panic(boom)
					}
				},
			},
		}

		evict := streamflight.WithOverflow(streamflight.Evict)
		var cut []*streamflight.Subscription[int]
		for range 3 {
			s, err := g.Subscribe("k", evict)
			x.NoError(err)
			cut = append(cut, s)
		}
		keep, err := g.Subscribe("k", streamflight.WithBuffer(8))
		x.NoError(err)
		r.emit("k", 1)
		x.PanicsWithValue(boom, func() { r.emit("k", 2) }, "the second eviction's hook")

		x.ErrorIs(cut[0].Err(), streamflight.ErrEvicted)
		x.ErrorIs(cut[1].Err(), streamflight.ErrEvicted)
		x.NoError(cut[2].Err(), "not reached, so still subscribed")

		var n int
		returns(t, func() { n = r.emit("k", 3) })
		x.Equal(1, n, "keep takes it, with nothing sent to a closed queue")
		x.ErrorIs(cut[2].Err(), streamflight.ErrEvicted, "the one not reached is evicted now")
		x.Equal([]int{1, 3}, drain(keep.C), "2 never got past the panic")
		for _, s := range append(cut, keep) {
			x.NoError(s.Close())
		}
	})
	t.Run("an Evicted hook that panics as a catch-up is cut gives the reference back", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Replay: 2,
			Hooks: streamflight.Hooks[string, int]{
				Evicted: func(string) { panic(boom) },
			},
		}

		keep, err := g.Subscribe("k", streamflight.WithBuffer(8))
		x.NoError(err)
		r.emit("k", 1, 2)
		x.PanicsWithValue(boom, func() {
			g.Subscribe("k", streamflight.WithOverflow(streamflight.Evict))
		})

		var n int
		returns(t, func() { n = r.emit("k", 3) })
		x.Equal(1, n, "the cut subscriber was never added")
		x.Equal([]int{1, 2, 3}, drain(keep.C))
		x.NoError(keep.Close())
		x.Equal([]string{"open k", "stop k"}, r.Log(), "and its reference was given back")
	})
	t.Run("an Ended hook that panics fails the End, not the key", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		var once atomic.Bool
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Ended: func(string, error) {
					if once.CompareAndSwap(false, true) {
						panic(boom)
					}
				},
			},
		}

		s, err := g.Subscribe("k")
		x.NoError(err)
		x.PanicsWithValue(boom, func() { r.emitter("k").End(nil) })
		x.ErrorIs(s.Err(), io.EOF, "it ended all the same")

		var next *streamflight.Subscription[int]
		returns(t, func() { next, err = g.Subscribe("k") })
		x.NoError(err)
		x.Equal([]string{"open k", "stop k", "open k"}, r.Log(), "a fresh upstream")
		x.NoError(next.Close())
		x.NoError(s.Close())
	})
	t.Run("a Stopped hook that panics in Close does not keep the rest from stopping", func(t *testing.T) {
		x := require.New(t)
		r := newRecorder()
		g := &streamflight.Group[string, int]{
			Source: r.Source,
			Hooks: streamflight.Hooks[string, int]{
				Stopped: func(string, error) { panic(boom) },
			},
		}

		a, err := g.Subscribe("a")
		x.NoError(err)
		b, err := g.Subscribe("b")
		x.NoError(err)

		x.PanicsWithValue(boom, func() { g.Close() })
		x.ElementsMatch([]string{"open a", "open b", "stop a", "stop b"}, r.Log(),
			"each stop panicked, and each still ran")
		x.ErrorIs(a.Err(), streamflight.ErrGroupClosed)
		x.ErrorIs(b.Err(), streamflight.ErrGroupClosed)

		returns(t, func() { err = g.Close() })
		x.NoError(err)
		x.NoError(a.Close())
		x.NoError(b.Close())
	})
	t.Run("a stop that panics in Close leaves later Closes the errors of the rest", func(t *testing.T) {
		x := require.New(t)
		before, after := errors.New("before"), errors.New("after")
		var stops atomic.Int64
		g := &streamflight.Group[string, int]{
			Source: func(string, streamflight.Emitter[int]) (func() error, error) {
				return func() error {
					switch stops.Add(1) { // in the order Close runs them
					case 1:
						return before
					case 2:
						panic(boom)
					}
					return after
				}, nil
			},
		}
		var subs []*streamflight.Subscription[int]
		for _, key := range []string{"a", "b", "c"} {
			s, err := g.Subscribe(key)
			x.NoError(err)
			subs = append(subs, s)
		}

		x.PanicsWithValue(boom, func() { g.Close() })
		var err error
		returns(t, func() { err = g.Close() })
		x.ErrorIs(err, before, "stopped before the panic, and not lost to it")
		x.ErrorIs(err, after, "stopped after it")
		x.Equal(int64(3), stops.Load())
		x.Equal(err, g.Close(), "the same error every time")
		for _, s := range subs {
			x.NoError(s.Close())
		}
	})
	t.Run("a stop that panics in Close still stops a key that was opening", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			x := require.New(t)
			r := newRecorder()
			release := make(chan struct{})
			failed := errors.New("b's stop failed")
			g := &streamflight.Group[string, int]{
				Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
					if key != "b" {
						return r.Source(key, e)
					}
					<-release
					stop, err := r.Source(key, e)
					return func() error {
						_ = stop()
						return failed
					}, err
				},
				Hooks: streamflight.Hooks[string, int]{
					Stopped: func(key string, _ error) {
						if key == "a" {
							panic(boom)
						}
					},
				},
			}

			a, err := g.Subscribe("a")
			x.NoError(err)
			opened := make(chan error, 1)
			go func() {
				_, err := g.Subscribe("b")
				opened <- err
			}()
			synctest.Wait() // b is opening

			closed := make(chan any, 1)
			go func() {
				defer func() { closed <- recover() }()
				g.Close()
			}()
			synctest.Wait() // a's stop has panicked, and Close waits on b all the same

			close(release)
			x.Equal(boom, <-closed)
			x.ErrorIs(<-opened, streamflight.ErrGroupClosed)
			x.Equal([]string{"open a", "stop a", "open b", "stop b"}, r.Log(),
				"b was stopped before Close gave up, since nobody else could stop it")

			var err2 error
			returns(t, func() { err2 = g.Close() })
			x.ErrorIs(err2, failed, "b's stop, run once a's had panicked, still counts")
			x.NoError(a.Close())
		})
	})
	t.Run("a second stop that panics while Close finishes still leaves nothing running", func(t *testing.T) {
		// Close waits on whichever opening key it finds first. Only when that
		// is b does b's stop panic inside Close's re-run while c is still
		// opening, the order that needs the re-run to guard itself too, and
		// scheduling makes that about one round in eight. So go round until
		// both orders have come up.
		nested, rounds := 0, 0
		for rounds < 1000 && (nested == 0 || nested == rounds) {
			rounds++
			synctest.Test(t, func(t *testing.T) {
				x := require.New(t)
				r := newRecorder()
				gates := map[string]chan struct{}{"b": make(chan struct{}), "c": make(chan struct{})}
				g := &streamflight.Group[string, int]{
					Source: func(key string, e streamflight.Emitter[int]) (func() error, error) {
						if gate, ok := gates[key]; ok {
							<-gate
						}
						return r.Source(key, e)
					},
					Hooks: streamflight.Hooks[string, int]{
						Stopped: func(key string, _ error) {
							if key != "c" {
								panic(boom + " " + key)
							}
						},
					},
				}

				a, err := g.Subscribe("a")
				x.NoError(err)
				opened := make(chan error, 2)
				for _, key := range []string{"b", "c"} {
					go func() {
						_, err := g.Subscribe(key)
						opened <- err
					}()
				}
				synctest.Wait() // b and c are opening

				closed := make(chan any, 1)
				go func() {
					defer func() { closed <- recover() }()
					g.Close()
				}()
				synctest.Wait()
				close(gates["b"])
				synctest.Wait()
				if slices.Contains(r.Log(), "stop b") {
					nested++ // stopped with c still opening
				}
				close(gates["c"])

				x.NotNil(<-closed)
				x.ErrorIs(<-opened, streamflight.ErrGroupClosed)
				x.ErrorIs(<-opened, streamflight.ErrGroupClosed)
				x.ElementsMatch([]string{"open a", "stop a", "open b", "stop b", "open c", "stop c"}, r.Log(),
					"every upstream stopped, whichever stops panicked on the way")

				var err2 error
				returns(t, func() { err2 = g.Close() })
				x.NoError(err2)
				x.NoError(a.Close())
			})
		}
		require.Positive(t, nested, "the order that needs the nested re-run never came up")
		require.Less(t, nested, rounds, "and neither did the other")
	})
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
