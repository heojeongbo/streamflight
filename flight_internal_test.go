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

// Of two Ends, the one that finishes the flight reports it, even when the
// other closed quit first: that other finds the flight ended and does
// nothing, and Ended would otherwise not be reported at all.
func TestEndsRacingAreEndedOnce(t *testing.T) {
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

	f.quitOnce.Do(f.endByItself) // the first End, before it has mu
	f.End(errors.New("second"))  // the second, which gets mu first
	x.Equal(1, ended, "reported by the End that finished it")
	f.End(errors.New("first")) // the first, resuming
	x.Equal(1, ended)
	x.EqualError(s.Err(), "second")
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
