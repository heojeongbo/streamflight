package streamflight

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// help can take a caller off the queue just as that caller gives up. Through
// the exported API only a race gets there, so the queue is set up by hand: a
// caller that has already given up, and a lock that help has to wait for.
func TestHelpPassesOverACallerThatGaveUp(t *testing.T) {
	x := require.New(t)
	g := &Group[string, int]{
		Source: func(string, Emitter[int]) (func() error, error) { return nil, nil },
	}
	s, err := g.Subscribe("k")
	x.NoError(err)
	f := g.flights["k"]

	gaveUp := make(chan struct{})
	close(gaveUp)
	f.mu.Lock()
	f.lockersMu.Lock()
	f.enqueue(&locker{got: make(chan struct{}), gaveUp: gaveUp})
	f.helping = true
	f.lockersMu.Unlock()
	go f.help()
	f.mu.Unlock()

	// Nobody takes the lock from help, so it must pass the caller over, find
	// the queue empty and let the lock go.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		f.lockersMu.Lock()
		helping := f.helping
		f.lockersMu.Unlock()
		if !helping && f.mu.TryLock() {
			f.mu.Unlock()
			break
		}
		x.False(time.Now().After(deadline), "help kept the key's lock")
	}
	x.NoError(s.Close())
}

// An End that comes once a stop has begun is the Group stopping the key, not
// the upstream ending by itself: Close begins every stop before it ends any
// key, and a key fed by another can end in reply before its own end comes.
// The window is narrow from outside, so the stop's first step is taken by
// hand here: the claim, which is what says a stop has begun, rather than the
// close of quit that the claim makes.
func TestEndOnceAStopHasBegunIsNotEnded(t *testing.T) {
	x := require.New(t)
	ended := 0
	g := &Group[string, int]{
		Source: func(string, Emitter[int]) (func() error, error) { return nil, nil },
		Hooks: Hooks[string, int]{
			Ended: func(string, error) { ended++ },
		},
	}
	s, err := g.Subscribe("k")
	x.NoError(err)
	f := g.flights["k"]

	g.mu.Lock()
	x.True(g.beginStop(f)) // what every stop does first
	g.mu.Unlock()

	f.End(errors.New("in reply"))
	x.Zero(ended)
	x.EqualError(s.Err(), "in reply", "its subscribers are ended all the same")
	x.NoError(g.doStop(f, nil))
	x.NoError(s.Close())
}

// A caller waiting on an open keeps what was opened from being stopped for
// having nobody, and the last such caller to give up leaves it as a last
// subscriber would. From outside only a race gets a waiter to give up between
// the opener letting go and the waiter joining, so the wait is set up by hand.
func TestGivingUpOnAnOpenNobodyHolds(t *testing.T) {
	x := require.New(t)
	stops := 0
	g := &Group[string, int]{
		Source: func(string, Emitter[int]) (func() error, error) {
			return func() error { stops++; return nil }, nil
		},
	}
	s, err := g.Subscribe("k")
	x.NoError(err)
	f := g.flights["k"]

	g.mu.Lock()
	f.pending++ // a caller waiting to join
	g.mu.Unlock()
	x.NoError(s.Close())
	x.Zero(stops, "kept for the caller about to join")

	g.giveUpWaiting(f)
	x.Equal(1, stops, "and stopped once that caller gave up")
}

// The queue of callers waiting for a key's lock: first in, first out, and a
// caller leaves it from anywhere, or not at all once help has taken it out.
func TestLockQueue(t *testing.T) {
	x := require.New(t)
	f := &flight[string, int]{}
	order := func() []*locker {
		var ls []*locker
		for l := f.first; l != nil; l = l.next {
			ls = append(ls, l)
		}
		return ls
	}
	a, b, c := &locker{}, &locker{}, &locker{}
	f.enqueue(a)
	f.enqueue(b)
	f.enqueue(c)
	x.Equal([]*locker{a, b, c}, order())

	f.dequeue(b) // from the middle
	x.Equal([]*locker{a, c}, order())
	f.dequeue(c) // from the back
	x.Equal([]*locker{a}, order())
	x.Same(a, f.last)
	f.dequeue(a) // the only one
	x.Nil(f.first)
	x.Nil(f.last)

	f.dequeue(a) // gone already: nothing happens
	x.Nil(f.first)
	f.enqueue(b)
	x.Equal([]*locker{b}, order(), "and the queue works again")
}

// An Emit that evicts leaves no reference to those it evicted in the spare
// capacity of the key's list, where they would outlive their Close.
func TestEvictionClearsWhatItMovesPast(t *testing.T) {
	x := require.New(t)
	g := &Group[string, int]{
		Source: func(string, Emitter[int]) (func() error, error) { return nil, nil },
	}
	var subs []*Subscription[int]
	for range 4 {
		s, err := g.Subscribe("k", WithOverflow(Evict))
		x.NoError(err)
		subs = append(subs, s)
	}
	keep, err := g.SubscribeFunc("k", func(int) {})
	x.NoError(err)
	f := g.flights["k"]

	f.Emit(1)
	f.Emit(2) // evicts all four channel subscribers
	x.Len(f.subs, 1)
	for _, s := range f.subs[1:cap(f.subs)] {
		x.Nil(s, "nothing left behind in the backing array")
	}
	for _, s := range append(subs, keep) {
		x.NoError(s.Close())
	}
}

// Of two Ends the first decides, even when the second reaches the key's lock
// first: the second finishes the flight, but with the first one's error and
// reporting the first one's end, which is what makes an End after the first
// one do nothing. Reported once, by whoever finishes.
func TestEndsRacingAreEndedOnce(t *testing.T) {
	x := require.New(t)
	var ended []error
	g := &Group[string, int]{
		Source: func(string, Emitter[int]) (func() error, error) { return nil, nil },
		Hooks: Hooks[string, int]{
			Ended: func(_ string, err error) { ended = append(ended, err) },
		},
	}
	s, err := g.Subscribe("k")
	x.NoError(err)
	f := g.flights["k"]

	first := errors.New("first")
	f.quitOnce.Do(func() { f.endByItself(first) }) // the first End, before it has mu
	f.End(errors.New("second"))                    // the second, which gets mu first
	x.Equal([]error{first}, ended, "the end the first one decided")
	f.End(first) // the first, resuming
	x.Len(ended, 1)
	x.EqualError(s.Err(), "first")
	x.NoError(s.Close())
}

// A caller that did not wait on an open, reaching the key before those that
// did, joins an upstream that ended as it opened rather than taking it over:
// otherwise each of them would find it stopping and open the key again.
func TestNewcomerSharesAnEndOthersWaitOn(t *testing.T) {
	x := require.New(t)
	opens, stops := 0, 0
	g := &Group[string, int]{
		Source: func(_ string, e Emitter[int]) (func() error, error) {
			opens++
			e.End(errors.New("refused"))
			return func() error { stops++; return nil }, nil
		},
	}
	a, err := g.Subscribe("k")
	x.NoError(err)
	f := g.flights["k"]

	g.mu.Lock()
	f.pending++ // a caller that waited on the open, not back for the lock yet
	g.mu.Unlock()
	c, err := g.Subscribe("k") // one that did not wait, first to the lock
	x.NoError(err)
	x.Equal(1, opens, "joined, not opened again")
	x.EqualError(c.Err(), "refused")

	x.NoError(a.Close())
	x.NoError(c.Close())
	x.Zero(stops, "kept for the caller about to join")
	g.giveUpWaiting(f)
	x.Equal(1, stops)
}

// A caller that gives up on an open once a Close has begun leaves the stop to
// Close, which returns what the stop func does: the caller would have nowhere
// to return it.
func TestGivingUpOnceCloseHasBegun(t *testing.T) {
	x := require.New(t)
	failed := errors.New("stop failed")
	g := &Group[string, int]{
		Source: func(string, Emitter[int]) (func() error, error) {
			return func() error { return failed }, nil
		},
	}
	s, err := g.Subscribe("k")
	x.NoError(err)
	f := g.flights["k"]
	g.mu.Lock()
	f.pending++ // a caller waiting to join
	g.mu.Unlock()
	x.NoError(s.Close(), "nothing stopped while it waits")

	g.mu.Lock()
	g.closeDone = make(chan struct{}) // as Close does first
	g.mu.Unlock()
	g.giveUpWaiting(f)
	x.Equal(live, f.st, "left for Close")

	var errs []error
	g.closeAll(&errs)
	x.ErrorIs(errors.Join(errs...), failed)
}

// A Linger timer that has fired as a takeover claims the key waits for the
// Group's lock, and must find the key claimed rather than stop it again: the
// takeover stops it without the lock, and until it is done the key is still
// the flight's. From outside the timer reaches the lock in that window only
// now and then, so the takeover's claim is made by hand.
func TestTimerFiringAsTheKeyIsTakenOver(t *testing.T) {
	x := require.New(t)
	stops := 0
	g := &Group[string, int]{
		Source: func(string, Emitter[int]) (func() error, error) {
			return func() error { stops++; return nil }, nil
		},
		Linger: time.Hour, // armed, but fired by hand below
	}
	s, err := g.Subscribe("k")
	x.NoError(err)
	x.NoError(s.Close())
	f := g.flights["k"]

	g.mu.Lock()
	gen := f.gen     // what the timer was armed with
	g.claimLocked(f) // as a takeover does, the timer already past Stop
	g.mu.Unlock()
	g.expire(f, gen)
	x.Zero(stops, "left to the takeover")
	x.Equal(stopping, f.st)

	x.NoError(g.doStop(f, nil)) // the takeover's stop
	x.Equal(1, stops)
}

// arrivedWithTheEnd is a key on which a value arrives and the subscription
// ends between Wait finding nothing new and Wait selecting, so that both are
// ready to select by then.
type arrivedWithTheEnd struct {
	calls int
	newer chan struct{}
	at    time.Time
}

func (o *arrivedWithTheEnd) leave(*Subscription[int]) error { return nil }
func (o *arrivedWithTheEnd) latest() (int, time.Time, bool) { return 7, o.at, true }
func (o *arrivedWithTheEnd) latestAfter(time.Time) (int, time.Time, bool, <-chan struct{}) {
	o.calls++
	if o.calls%2 == 1 {
		return 0, time.Time{}, false, o.newer
	}
	return 7, o.at, true, nil
}

// A Wait that finds the end and a newer value both ready returns the value,
// whichever select picks: the subscription has not ended with no such value.
func TestWaitSeesAValueThatArrivedWithTheEnd(t *testing.T) {
	x := require.New(t)
	o := &arrivedWithTheEnd{newer: make(chan struct{}), at: time.Unix(1, 0)}
	close(o.newer)
	s := newSubscription[int](sampled, nil, nil, DropOldest)
	s.owner = o
	close(s.done)

	for range 100 { // select would pick the end about half the time
		v, at, ok := s.Wait(t.Context(), time.Time{})
		x.True(ok)
		x.Equal(7, v)
		x.Equal(o.at, at)
	}
}

// end makes why a subscription ended visible before it closes the queue, so a
// reader ranging over C finds the reason as soon as C closes. The two closes
// are too close together for a race to tell their order, so the queue is
// closed beforehand and end's own close of it panics: why must be out by then.
func TestEndPublishesWhyBeforeClosingTheQueue(t *testing.T) {
	x := require.New(t)
	s := newSubscription[int](queued, nil, make(chan int, 1), DropOldest)
	close(s.ch)

	x.Panics(func() { s.end(io.EOF) })
	x.ErrorIs(s.Err(), io.EOF)
}

// An End decides the flight is over where it closes quit, not where it reaches
// the key's lock. A stop that takes the lock in between finishes the flight,
// and the end is owed all the same: Ended is the only thing that carries why a
// stream died, so losing it leaves a caller nothing to read. The window is a
// few instructions from outside, so the End's first step is taken by hand.
func TestEndOvertakenByAStopIsStillReported(t *testing.T) {
	boom := errors.New("upstream gave up")

	t.Run("by the last subscriber leaving", func(t *testing.T) {
		x := require.New(t)
		var ended []error
		g := &Group[string, int]{
			Source: func(string, Emitter[int]) (func() error, error) { return nil, nil },
			Hooks:  Hooks[string, int]{Ended: func(_ string, err error) { ended = append(ended, err) }},
		}
		s, err := g.Subscribe("k")
		x.NoError(err)
		f := g.flights["k"]

		f.quitOnce.Do(func() { f.endByItself(boom) }) // the End, before it has mu
		x.NoError(s.Close())                          // stops it, reaching mu first

		x.Equal([]error{boom}, ended)
		f.End(boom) // the End, resuming: nothing left to do
		x.Len(ended, 1)
	})
	t.Run("by Group.Close, which keeps the upstream's own error", func(t *testing.T) {
		x := require.New(t)
		var ended []error
		g := &Group[string, int]{
			Source: func(string, Emitter[int]) (func() error, error) { return nil, nil },
			Hooks:  Hooks[string, int]{Ended: func(_ string, err error) { ended = append(ended, err) }},
		}
		s, err := g.Subscribe("k")
		x.NoError(err)
		f := g.flights["k"]

		f.quitOnce.Do(func() { f.endByItself(boom) })
		x.NoError(g.Close())

		x.Equal([]error{boom}, ended)
		x.ErrorIs(s.Err(), boom, "it had ended before the Close began, so that is why")
		x.NoError(s.Close())
	})
}

// An upstream that has ended by itself when its last subscriber leaves does not
// linger: there is nothing left to come back to, and holding the stop func for
// Linger holds whatever it closes with it.
func TestAnEndedUpstreamDoesNotLinger(t *testing.T) {
	x := require.New(t)
	stops := 0
	g := &Group[string, int]{
		Linger: time.Hour,
		Source: func(string, Emitter[int]) (func() error, error) {
			return func() error { stops++; return nil }, nil
		},
	}
	s, err := g.Subscribe("k")
	x.NoError(err)
	f := g.flights["k"]

	f.quitOnce.Do(func() { f.endByItself(errors.New("gone")) }) // the End, before it has mu
	x.NoError(s.Close())

	g.mu.Lock()
	armed, held := f.timer != nil, g.flights["k"] != nil
	g.mu.Unlock()
	x.False(armed, "an upstream that has ended has nothing to linger for")
	x.False(held, "stopped, and the key given back")
	x.Equal(1, stops)
	x.NoError(g.Close())
}

// A value emitted after the upstream has ended is dropped, whether or not the
// End that ended it has reached the key's lock yet.
func TestAValueEmittedAfterTheEndIsDropped(t *testing.T) {
	x := require.New(t)
	g := &Group[string, int]{
		Replay: 4,
		Source: func(string, Emitter[int]) (func() error, error) { return nil, nil },
	}
	s, err := g.Subscribe("k", WithBuffer(4))
	x.NoError(err)
	f := g.flights["k"]

	x.Equal(1, f.Emit(1), "before the end it is delivered")
	f.quitOnce.Do(func() { f.endByItself(io.EOF) }) // the End, before it has mu
	x.Zero(f.Emit(2), "the upstream has ended")
	x.Equal(1, f.count, "and it was not kept for Replay either")

	f.End(io.EOF)
	x.Equal([]int{1}, drainInts(s.C))
	x.NoError(s.Close())
}

func drainInts(c <-chan int) []int {
	var out []int
	for v := range c {
		out = append(out, v)
	}
	return out
}

// The Ended hook is the caller's code, so it can panic, and a stop is where it
// is reported for every ending but an End's own. That must not cost the
// upstream its stop: the key is given back as the stop returns, so after that
// nothing can ever reach what the Source opened again.
func TestEndedHookThatPanicsStillStops(t *testing.T) {
	x := require.New(t)
	stops, stopped := 0, 0
	g := &Group[string, int]{
		Source: func(string, Emitter[int]) (func() error, error) {
			return func() error { stops++; return nil }, nil
		},
		Hooks: Hooks[string, int]{
			Ended:   func(string, error) { panic("ended hook") },
			Stopped: func(string, error) { stopped++ },
		},
	}
	s, err := g.Subscribe("k")
	x.NoError(err)
	f := g.flights["k"]

	f.quitOnce.Do(func() { f.endByItself(errors.New("gone")) }) // the End, before it has mu
	x.PanicsWithValue("ended hook", func() { _ = s.Close() })   // the stop it is reported in

	x.Equal(1, stops, "the upstream was stopped all the same")
	x.Equal(1, stopped, "and reported")
	x.Empty(g.flights, "and the key given back")
	x.NoError(g.Close())
}

// Nor may it strand a closing Group. Close ends every key it claims before it
// stops any, some on goroutines of its own where a panic would have no call to
// fail, so the end is left for the stop that follows to report, on Close's own
// goroutine. Every key it claimed is stopped, and the panic fails that Close.
func TestEndedHookThatPanicsFailsCloseWithEverythingStopped(t *testing.T) {
	x := require.New(t)
	stops := 0
	g := &Group[string, int]{
		Source: func(string, Emitter[int]) (func() error, error) {
			return func() error { stops++; return nil }, nil
		},
		Hooks: Hooks[string, int]{Ended: func(string, error) { panic("ended hook") }},
	}
	held, err := g.Subscribe("held") // ended on a goroutine of Close's own
	x.NoError(err)
	free, err := g.Subscribe("free") // ended on Close's goroutine
	x.NoError(err)

	for _, key := range []string{"held", "free"} {
		f := g.flights[key]
		f.quitOnce.Do(func() { f.endByItself(errors.New("gone")) })
	}
	g.flights["held"].mu.Lock() // Close cannot have it at once

	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		_ = g.Close()
	}()
	time.Sleep(10 * time.Millisecond) // Close is waiting for the held key
	g.flights["held"].mu.Unlock()

	select {
	case r := <-done:
		x.Equal("ended hook", r, "the panic fails the Close it ran in")
	case <-time.After(5 * time.Second):
		x.Fail("Close never returned")
	}
	x.Equal(2, stops, "both upstreams were stopped")
	x.Empty(g.flights, "and both keys given back")
	x.NoError(g.Close(), "a later Close has nothing left to do")
	x.NoError(held.Close())
	x.NoError(free.Close())
}
