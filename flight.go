package streamflight

import (
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// flight is the upstream of one key: its Source's Emitter and the set of
// subscribers it delivers to.
type flight[K comparable, T any] struct {
	g    *Group[K, T]
	key  K
	stop func() error

	// quit is closed when the flight ends or is stopped, so an Emit waiting on
	// a Block subscriber gives up before the ending needs mu.
	quit     chan struct{}
	quitOnce sync.Once

	// Guarded by g.mu.
	refs  int
	gen   uint64 // bumped whenever a linger timer is armed or disarmed
	timer *time.Timer
	st    state
	wait  chan struct{} // closed when f leaves the phase it is in

	// openErr is why this flight never opened. Written under g.mu before wait
	// is closed, and read only after receiving from it.
	openErr error

	// ended mirrors done so the Group can tell whether this flight has ended
	// without waiting for a delivery that is holding mu. Written under mu.
	ended atomic.Bool

	// The newest value, for the subscribers that sample instead of being
	// delivered to. One per key rather than one per subscriber: they all want
	// the same answer, so Emit stores it once however many are watching.
	//
	// latestMu is a leaf, taken only around these three fields and never while
	// mu is wanted, so a reader is never behind a delivery. wanted is set by
	// the first such subscriber, from the moment it starts opening the key if
	// it is the one that opens it, and guards the cost of the clock read for
	// every key that has none.
	latestMu sync.RWMutex
	latestV  T
	latestAt time.Time
	latestOK bool
	latestCh chan struct{} // closed and replaced whenever the value advances
	wanted   bool          // guarded by mu, once f is published

	// mu guards the fields below and is held for every delivery.
	mu     sync.Mutex
	subs   []*Subscription[T]
	ring   []T // the latest values, for Replay
	head   int // where the next value goes in ring
	count  int // how many values ring holds
	done   bool
	endErr error
}

type outcome int

const (
	accepted outcome = iota
	rejected
	evicted
)

func newFlight[K comparable, T any](g *Group[K, T], key K) *flight[K, T] {
	return &flight[K, T]{
		g:    g,
		key:  key,
		quit: make(chan struct{}),
	}
}

// replay sizes the ring this flight remembers for a late subscriber. Called by
// open, before the Source can emit and while nothing else can reach f, so it
// needs no lock and can run the caller's ReplayFor outside the Group's.
func (f *flight[K, T]) replay() {
	n := f.g.Replay
	if f.g.ReplayFor != nil {
		n = f.g.ReplayFor(f.key)
	}
	if n > 0 {
		f.ring = make([]T, n)
	}
}

func (f *flight[K, T]) Emit(v T) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		return 0
	}
	if len(f.ring) > 0 {
		f.ring[f.head] = v
		f.head = (f.head + 1) % len(f.ring)
		f.count = min(f.count+1, len(f.ring))
	}

	if f.wanted {
		f.latestMu.Lock()
		// Strictly after the one before it, even when the clock did not move
		// between them: two values a caller can tell apart must have arrival
		// times it can tell apart, or waiting for one past the other never
		// ends. The nudge is a nanosecond and only under a clock too coarse to
		// separate two emissions.
		at := f.g.now()
		if f.latestOK && !at.After(f.latestAt) {
			at = f.latestAt.Add(time.Nanosecond)
		}
		f.latestV, f.latestAt, f.latestOK = v, at, true
		if f.latestCh != nil {
			close(f.latestCh)
			f.latestCh = nil
		}
		f.latestMu.Unlock()
	}

	n := 0
	for i := 0; i < len(f.subs); {
		s := f.subs[i]
		switch f.push(s, v, s.overflow) {
		case accepted:
			n++
		case evicted:
			f.subs = slices.Delete(f.subs, i, i+1)
			s.end(ErrEvicted)
			continue
		}
		i++
	}
	return n
}

func (f *flight[K, T]) End(err error) {
	if err == nil {
		err = io.EOF
	}
	f.unblock()

	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.done {
		f.finish(err)
	}
}

// unblock lets an Emit waiting on a Block subscriber go.
func (f *flight[K, T]) unblock() {
	f.quitOnce.Do(func() { close(f.quit) })
}

// waitLocked returns a channel closed when f leaves the phase it is in. The
// caller must have observed that phase in the same critical section, so the
// channel it gets is the one the phase's own exit closes. g.mu must be held.
func (f *flight[K, T]) waitLocked() chan struct{} {
	if f.wait == nil {
		f.wait = make(chan struct{})
	}
	return f.wait
}

// wakeLocked releases everyone waiting on the phase f is leaving. g.mu must be
// held.
func (f *flight[K, T]) wakeLocked() {
	if f.wait != nil {
		close(f.wait)
		f.wait = nil
	}
}

// finish ends every subscriber with err and drops what is kept for Replay.
// f.mu must be held.
func (f *flight[K, T]) finish(err error) {
	// Before anything else: the Group reads this without taking f.mu, which is
	// what keeps a stalled delivery from stalling every other key. Do not
	// derive it from quit instead: End closes quit before calling finish, so a
	// quit-based check would let the Group end subscribers with a nil reason in
	// between, rather than with err.
	f.ended.Store(true)
	f.done = true
	f.endErr = err
	for _, s := range f.subs {
		s.end(err)
	}
	f.subs = nil
	f.ring = nil
	f.count = 0
}

// attach adds s, first sending it what Replay and Initial have for it.
func (f *flight[K, T]) attach(s *Subscription[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		s.end(f.endErr)
		return
	}

	if s.fn == nil && s.ch == nil {
		// From here the key keeps its newest value. Not unset when this
		// subscriber leaves: the next one would otherwise find nothing where
		// the one before it was reading.
		f.wanted = true
	}

	// Never wait on a subscriber that cannot read yet: it is still inside
	// Subscribe. A queue shorter than what is sent keeps the newest, unless a
	// gap would corrupt what follows it: Evict ends the subscriber for a gap in
	// what it catches up on, as it would for one in what arrives live.
	policy := DropOldest
	if s.overflow == Evict {
		policy = Evict
	}
	cut := false
	for i := 0; i < f.count && !cut; i++ {
		cut = f.push(s, f.ring[(f.head-f.count+i+len(f.ring))%len(f.ring)], policy) == evicted
	}
	if f.g.Initial != nil && !cut {
		cut = f.initial(s, policy)
	}
	if cut {
		// Never added, so Close only has to give back its reference.
		s.end(ErrEvicted)
		return
	}
	f.subs = append(f.subs, s)
}

// initial sends s what Initial has for it, and reports whether that cut s off.
// A method of its own so that what its send captures is allocated only for a
// Group that has an Initial. f.mu must be held.
func (f *flight[K, T]) initial(s *Subscription[T], policy Overflow) (cut bool) {
	// send borrows the key's lock, which is held only for this call. A retained
	// send would otherwise deliver without it, racing the whole key and
	// panicking on a queue that has since been closed. Deferred, so a send kept
	// by an Initial that panicked is refused too.
	var live atomic.Bool
	live.Store(true)
	defer live.Store(false)
	f.g.Initial(f.key, func(v T) {
		if !live.Load() {
			panic("streamflight: Group.Initial called send after returning")
		}
		if !cut {
			cut = f.push(s, v, policy) == evicted
		}
	})
	return cut
}

func (f *flight[K, T]) leave(s *Subscription[T]) error {
	f.mu.Lock()
	if i := slices.Index(f.subs, s); i >= 0 {
		f.subs = slices.Delete(f.subs, i, i+1)
		s.end(ErrClosed)
	}
	f.mu.Unlock()

	return f.g.release(f)
}

// latest is the newest value of this key, when it arrived, and whether there
// is one. It takes no lock a delivery can hold.
func (f *flight[K, T]) latest() (T, time.Time, bool) {
	f.latestMu.RLock()
	defer f.latestMu.RUnlock()
	return f.latestV, f.latestAt, f.latestOK
}

// latestAfter returns the newest value if it arrived after t, and otherwise a
// channel closed when a newer one does. Both under one lock, so a value that
// lands between looking and waiting wakes the waiter rather than being missed.
func (f *flight[K, T]) latestAfter(t time.Time) (T, time.Time, bool, <-chan struct{}) {
	f.latestMu.Lock()
	defer f.latestMu.Unlock()

	if f.latestOK && f.latestAt.After(t) {
		return f.latestV, f.latestAt, true, nil
	}
	if f.latestCh == nil {
		f.latestCh = make(chan struct{})
	}
	var zero T
	return zero, time.Time{}, false, f.latestCh
}

// push delivers v to s under the given policy. f.mu must be held.
func (f *flight[K, T]) push(s *Subscription[T], v T, policy Overflow) outcome {
	if s.fn != nil {
		s.fn(v)
		return accepted
	}
	if s.ch == nil {
		// A sampling subscriber. Emit already stored the value for the whole
		// key, so there is nothing to hand this one.
		return accepted
	}

	select {
	case s.ch <- v:
		return accepted
	default:
	}

	switch policy {
	case DropNewest:
		f.drop(s, v)
		return rejected

	case Block:
		select {
		case s.ch <- v:
			return accepted
		case <-s.closing:
		case <-f.quit:
		}
		return rejected

	case Evict:
		return evicted

	default: // DropOldest
		select {
		case old := <-s.ch:
			f.drop(s, old)
		default:
			// The subscriber made room itself in the meantime.
		}
		// Cannot block: a slot was just freed, by the receive above or by the
		// subscriber, and only this goroutine sends to the queue.
		s.ch <- v
		return accepted
	}
}

func (f *flight[K, T]) drop(s *Subscription[T], v T) {
	s.dropped.Add(1)
	if f.g.Hooks.Dropped != nil {
		f.g.Hooks.Dropped(f.key, v)
	}
}
