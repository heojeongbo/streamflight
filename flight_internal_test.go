package streamflight

import (
	"errors"
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
// The window is too narrow to hit reliably from outside, so the stop's first
// step is taken by hand.
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

	f.unblock() // what every stop does first
	f.End(errors.New("in reply"))
	x.Zero(ended)
	x.EqualError(s.Err(), "in reply", "its subscribers are ended all the same")
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
